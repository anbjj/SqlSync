using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;

var options = CliOptions.Parse(args);
if (options.ShowHelp)
{
    CliOptions.PrintHelp();
    return;
}

try
{
    var connectionStore = new ConnectionStringStore("connstr");
    var resolvedConnectionString = await ResolveConnectionStringAsync(options, connectionStore);
    if (string.IsNullOrWhiteSpace(resolvedConnectionString))
    {
        Console.Error.WriteLine("Missing connection string. Use --connection, --connection-name, or SQLSYNC_CONNECTION_STRING.");
        CliOptions.PrintHelp();
        Environment.ExitCode = 1;
        return;
    }

    var resolvedOptions = options with { ConnectionString = resolvedConnectionString };
    var generator = new ContextScriptGenerator(resolvedOptions);
    await generator.GenerateAsync();
    Console.WriteLine($"Wrote context script to {resolvedOptions.OutputPath}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed: {ex.Message}");
    Environment.ExitCode = 1;
}

static async Task<string> ResolveConnectionStringAsync(CliOptions options, ConnectionStringStore store)
{
    var connectionString = options.ConnectionString;

    if (!string.IsNullOrWhiteSpace(options.ConnectionName))
    {
        connectionString = await store.LoadAsync(options.ConnectionName);
        Console.WriteLine($"Loaded connection profile '{options.ConnectionName}' from connstr/.");
    }

    if (!string.IsNullOrWhiteSpace(options.SaveConnectionName))
    {
        if (string.IsNullOrWhiteSpace(connectionString))
        {
            throw new ArgumentException("--save-connection requires --connection, --connection-name, or SQLSYNC_CONNECTION_STRING.");
        }

        await store.SaveAsync(options.SaveConnectionName, connectionString);
        Console.WriteLine($"Saved connection profile '{options.SaveConnectionName}' to connstr/.");
    }

    return connectionString;
}

internal sealed record CliOptions(
    string ConnectionString,
    string OutputPath,
    bool IncludeData,
    int TopRows,
    IReadOnlySet<string>? TableFilter,
    string? ConnectionName,
    string? SaveConnectionName,
    bool ShowHelp)
{
    private const string DefaultOutput = "sqlsync-context.sql";

    public static CliOptions Parse(string[] args)
    {
        string connection = Environment.GetEnvironmentVariable("SQLSYNC_CONNECTION_STRING") ?? string.Empty;
        string output = DefaultOutput;
        bool includeData = true;
        int topRows = 10;
        HashSet<string>? tableFilter = null;
        string? connectionName = null;
        string? saveConnectionName = null;
        bool showHelp = false;

        for (var i = 0; i < args.Length; i++)
        {
            var arg = args[i];
            switch (arg)
            {
                case "-h":
                case "--help":
                    showHelp = true;
                    break;

                case "-c":
                case "--connection":
                    connection = ReadValue(args, ref i, arg);
                    break;

                case "-o":
                case "--output":
                    output = ReadValue(args, ref i, arg);
                    break;

                case "--schema-only":
                    includeData = false;
                    break;

                case "--include-data":
                    includeData = true;
                    break;

                case "-t":
                case "--top":
                    if (!int.TryParse(ReadValue(args, ref i, arg), out topRows) || topRows < 0)
                    {
                        throw new ArgumentException("--top must be an integer >= 0");
                    }
                    break;

                case "--tables":
                    tableFilter = ParseTableFilter(ReadValue(args, ref i, arg));
                    break;

                case "--connection-name":
                    connectionName = ReadValue(args, ref i, arg);
                    break;

                case "--save-connection":
                    saveConnectionName = ReadValue(args, ref i, arg);
                    break;

                default:
                    throw new ArgumentException($"Unknown option: {arg}");
            }
        }

        return new CliOptions(connection, output, includeData, topRows, tableFilter, connectionName, saveConnectionName, showHelp);
    }

    public static void PrintHelp()
    {
        Console.WriteLine(
            """
            SqlSync - SQL schema and sample data extractor

            Usage:
              SqlSync --connection "<SQL Server connection string>" [options]

            Options:
              -c, --connection <value>    SQL Server connection string (or SQLSYNC_CONNECTION_STRING)
                  --connection-name <n>   Load connection from connstr/<n>.txt
                  --save-connection <n>   Save resolved connection to connstr/<n>.txt
              -o, --output <path>         Output .sql path (default: sqlsync-context.sql)
              -t, --top <n>               Top N sample rows per table (default: 10)
                  --schema-only           Export schema only
                  --include-data          Export schema + sample data (default)
                  --tables <list>         Comma-separated table names (dbo.Users,[sales].[Orders])
              -h, --help                  Show this help
            """);
    }

    private static string ReadValue(string[] args, ref int index, string option)
    {
        if (index + 1 >= args.Length)
        {
            throw new ArgumentException($"Missing value for {option}");
        }

        index++;
        return args[index];
    }

    private static HashSet<string> ParseTableFilter(string input)
    {
        var values = input
            .Split(',', StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries)
            .Select(NormalizeQualifiedObjectName);

        return new HashSet<string>(values, StringComparer.OrdinalIgnoreCase);
    }

    private static string NormalizeQualifiedObjectName(string rawName)
    {
        var trimmed = rawName.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            throw new ArgumentException("Object name cannot be empty.");
        }

        var parts = SplitQualifiedName(trimmed);
        if (parts.Count == 1)
        {
            return $"dbo.{NormalizeIdentifier(parts[0])}";
        }

        if (parts.Count == 2)
        {
            return $"{NormalizeIdentifier(parts[0])}.{NormalizeIdentifier(parts[1])}";
        }

        throw new ArgumentException($"Invalid object name '{rawName}'. Use name, schema.name, or [schema].[name].");
    }

    private static List<string> SplitQualifiedName(string value)
    {
        var parts = new List<string>();
        var current = new StringBuilder();
        var inBrackets = false;

        foreach (var ch in value)
        {
            if (ch == '[')
            {
                inBrackets = true;
                current.Append(ch);
                continue;
            }

            if (ch == ']')
            {
                inBrackets = false;
                current.Append(ch);
                continue;
            }

            if (ch == '.' && !inBrackets)
            {
                parts.Add(current.ToString().Trim());
                current.Clear();
                continue;
            }

            current.Append(ch);
        }

        if (inBrackets)
        {
            throw new ArgumentException($"Invalid object name '{value}'. Missing closing bracket.");
        }

        if (current.Length > 0)
        {
            parts.Add(current.ToString().Trim());
        }

        return parts.Where(static p => !string.IsNullOrWhiteSpace(p)).ToList();
    }

    private static string NormalizeIdentifier(string value)
    {
        var trimmed = value.Trim();
        if (trimmed.StartsWith('[', StringComparison.Ordinal) && trimmed.EndsWith(']', StringComparison.Ordinal))
        {
            trimmed = trimmed[1..^1].Replace("]]", "]", StringComparison.Ordinal);
        }

        if (string.IsNullOrWhiteSpace(trimmed))
        {
            throw new ArgumentException("Identifier cannot be empty.");
        }

        return trimmed;
    }
}

internal sealed class ConnectionStringStore
{
    private readonly string _directoryPath;

    public ConnectionStringStore(string directoryPath)
    {
        _directoryPath = directoryPath;
    }

    public async Task SaveAsync(string profileName, string connectionString)
    {
        var path = GetProfilePath(profileName);
        Directory.CreateDirectory(_directoryPath);
        await File.WriteAllTextAsync(path, connectionString.Trim());
    }

    public async Task<string> LoadAsync(string profileName)
    {
        var path = GetProfilePath(profileName);
        if (!File.Exists(path))
        {
            throw new FileNotFoundException($"Connection profile '{profileName}' was not found in {_directoryPath}.", path);
        }

        return (await File.ReadAllTextAsync(path)).Trim();
    }

    private string GetProfilePath(string profileName)
    {
        var safeName = ValidateProfileName(profileName);
        return Path.Combine(_directoryPath, $"{safeName}.txt");
    }

    private static string ValidateProfileName(string profileName)
    {
        var trimmed = profileName.Trim();
        if (string.IsNullOrWhiteSpace(trimmed))
        {
            throw new ArgumentException("Connection profile name cannot be empty.");
        }

        if (trimmed.Contains('/') || trimmed.Contains('\\') || trimmed.Contains("..", StringComparison.Ordinal))
        {
            throw new ArgumentException("Connection profile name cannot include path separators.");
        }

        var invalidChars = Path.GetInvalidFileNameChars();
        if (trimmed.IndexOfAny(invalidChars) >= 0)
        {
            throw new ArgumentException("Connection profile name contains invalid filename characters.");
        }

        return trimmed;
    }
}

internal sealed class ContextScriptGenerator
{
    private readonly CliOptions _options;

    public ContextScriptGenerator(CliOptions options)
    {
        _options = options;
    }

    public async Task GenerateAsync()
    {
        await using var connection = new SqlConnection(_options.ConnectionString);
        await connection.OpenAsync();

        var databaseName = await GetDatabaseNameAsync(connection);
        var tables = await GetTablesAsync(connection);

        if (_options.TableFilter is { Count: > 0 })
        {
            tables = tables
                .Where(t => _options.TableFilter.Contains($"{t.Schema}.{t.Name}"))
                .ToList();
        }

        var sb = new StringBuilder();
        sb.AppendLine($"-- SqlSync context script");
        sb.AppendLine($"-- Database: {databaseName}");
        sb.AppendLine($"-- Generated (UTC): {DateTime.UtcNow:yyyy-MM-dd HH:mm:ss}");
        sb.AppendLine();

        foreach (var table in tables)
        {
            var columns = await GetColumnsAsync(connection, table);
            var primaryKey = await GetPrimaryKeyAsync(connection, table);
            var foreignKeys = await GetForeignKeysAsync(connection, table);

            AppendCreateTable(sb, table, columns, primaryKey);
            AppendForeignKeys(sb, table, foreignKeys);

            if (_options.IncludeData && _options.TopRows > 0)
            {
                await AppendSampleDataAsync(sb, connection, table, columns);
            }

            sb.AppendLine();
        }

        var views = await GetViewsAsync(connection);
        AppendProgrammableObjectsSection(sb, "Views", views);

        var procedures = await GetStoredProceduresAsync(connection);
        AppendProgrammableObjectsSection(sb, "Stored Procedures", procedures);

        var functions = await GetFunctionsAsync(connection);
        AppendProgrammableObjectsSection(sb, "Functions", functions);

        var users = await GetUsersAsync(connection);
        AppendUsersSection(sb, users);

        await File.WriteAllTextAsync(_options.OutputPath, sb.ToString());
    }

    private static async Task<string> GetDatabaseNameAsync(SqlConnection connection)
    {
        await using var cmd = new SqlCommand("SELECT DB_NAME();", connection);
        return (string)(await cmd.ExecuteScalarAsync() ?? "(unknown)");
    }

    private static async Task<List<DbTable>> GetTablesAsync(SqlConnection connection)
    {
        const string sql =
            """
            SELECT s.name AS SchemaName, t.name AS TableName
            FROM sys.tables t
            INNER JOIN sys.schemas s ON s.schema_id = t.schema_id
            WHERE t.is_ms_shipped = 0
            ORDER BY s.name, t.name;
            """;

        await using var cmd = new SqlCommand(sql, connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var tables = new List<DbTable>();
        while (await reader.ReadAsync())
        {
            tables.Add(new DbTable(
                reader.GetString(reader.GetOrdinal("SchemaName")),
                reader.GetString(reader.GetOrdinal("TableName"))));
        }

        return tables;
    }

    private static async Task<List<DbColumn>> GetColumnsAsync(SqlConnection connection, DbTable table)
    {
        const string sql =
            """
            SELECT
                c.column_id,
                c.name AS ColumnName,
                ty.name AS DataType,
                c.max_length,
                c.precision,
                c.scale,
                c.is_nullable,
                c.is_identity,
                ISNULL(CONVERT(varchar(50), ic.seed_value), '1') AS identity_seed,
                ISNULL(CONVERT(varchar(50), ic.increment_value), '1') AS identity_increment,
                c.is_computed,
                cc.definition AS computed_definition,
                cc.is_persisted,
                dc.definition AS default_definition
            FROM sys.columns c
            INNER JOIN sys.types ty ON c.user_type_id = ty.user_type_id
            LEFT JOIN sys.identity_columns ic ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            LEFT JOIN sys.computed_columns cc ON c.object_id = cc.object_id AND c.column_id = cc.column_id
            LEFT JOIN sys.default_constraints dc ON c.default_object_id = dc.object_id
            WHERE c.object_id = OBJECT_ID(@FullTableName)
            ORDER BY c.column_id;
            """;

        await using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@FullTableName", $"[{table.Schema}].[{table.Name}]");
        await using var reader = await cmd.ExecuteReaderAsync();

        var columns = new List<DbColumn>();
        while (await reader.ReadAsync())
        {
            columns.Add(new DbColumn(
                Ordinal: reader.GetInt32(reader.GetOrdinal("column_id")),
                Name: reader.GetString(reader.GetOrdinal("ColumnName")),
                DataType: reader.GetString(reader.GetOrdinal("DataType")),
                MaxLength: reader.GetInt16(reader.GetOrdinal("max_length")),
                Precision: reader.GetByte(reader.GetOrdinal("precision")),
                Scale: reader.GetByte(reader.GetOrdinal("scale")),
                IsNullable: reader.GetBoolean(reader.GetOrdinal("is_nullable")),
                IsIdentity: reader.GetBoolean(reader.GetOrdinal("is_identity")),
                IdentitySeed: reader.GetString(reader.GetOrdinal("identity_seed")),
                IdentityIncrement: reader.GetString(reader.GetOrdinal("identity_increment")),
                IsComputed: reader.GetBoolean(reader.GetOrdinal("is_computed")),
                ComputedDefinition: reader.IsDBNull(reader.GetOrdinal("computed_definition"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("computed_definition")),
                IsPersistedComputed: !reader.IsDBNull(reader.GetOrdinal("is_persisted"))
                    && reader.GetBoolean(reader.GetOrdinal("is_persisted")),
                DefaultDefinition: reader.IsDBNull(reader.GetOrdinal("default_definition"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("default_definition"))
            ));
        }

        return columns;
    }

    private static async Task<List<string>> GetPrimaryKeyAsync(SqlConnection connection, DbTable table)
    {
        const string sql =
            """
            SELECT c.name AS ColumnName
            FROM sys.key_constraints kc
            INNER JOIN sys.index_columns ic
                ON kc.parent_object_id = ic.object_id AND kc.unique_index_id = ic.index_id
            INNER JOIN sys.columns c
                ON c.object_id = ic.object_id AND c.column_id = ic.column_id
            WHERE kc.parent_object_id = OBJECT_ID(@FullTableName)
              AND kc.type = 'PK'
            ORDER BY ic.key_ordinal;
            """;

        await using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@FullTableName", $"[{table.Schema}].[{table.Name}]");
        await using var reader = await cmd.ExecuteReaderAsync();

        var keys = new List<string>();
        while (await reader.ReadAsync())
        {
            keys.Add(reader.GetString(reader.GetOrdinal("ColumnName")));
        }

        return keys;
    }

    private static async Task<List<DbForeignKey>> GetForeignKeysAsync(SqlConnection connection, DbTable table)
    {
        const string sql =
            """
            SELECT
                fk.name AS ForeignKeyName,
                c_parent.name AS ParentColumnName,
                s_ref.name AS ReferencedSchema,
                t_ref.name AS ReferencedTable,
                c_ref.name AS ReferencedColumnName,
                fkc.constraint_column_id AS Ordinal
            FROM sys.foreign_keys fk
            INNER JOIN sys.foreign_key_columns fkc ON fk.object_id = fkc.constraint_object_id
            INNER JOIN sys.tables t_parent ON fk.parent_object_id = t_parent.object_id
            INNER JOIN sys.schemas s_parent ON t_parent.schema_id = s_parent.schema_id
            INNER JOIN sys.columns c_parent ON c_parent.object_id = fkc.parent_object_id AND c_parent.column_id = fkc.parent_column_id
            INNER JOIN sys.tables t_ref ON fk.referenced_object_id = t_ref.object_id
            INNER JOIN sys.schemas s_ref ON t_ref.schema_id = s_ref.schema_id
            INNER JOIN sys.columns c_ref ON c_ref.object_id = fkc.referenced_object_id AND c_ref.column_id = fkc.referenced_column_id
            WHERE fk.parent_object_id = OBJECT_ID(@FullTableName)
            ORDER BY fk.name, fkc.constraint_column_id;
            """;

        await using var cmd = new SqlCommand(sql, connection);
        cmd.Parameters.AddWithValue("@FullTableName", $"[{table.Schema}].[{table.Name}]");
        await using var reader = await cmd.ExecuteReaderAsync();

        var grouped = new Dictionary<string, DbForeignKeyBuilder>(StringComparer.OrdinalIgnoreCase);
        while (await reader.ReadAsync())
        {
            var name = reader.GetString(reader.GetOrdinal("ForeignKeyName"));
            if (!grouped.TryGetValue(name, out var fk))
            {
                fk = new DbForeignKeyBuilder(
                    name,
                    reader.GetString(reader.GetOrdinal("ReferencedSchema")),
                    reader.GetString(reader.GetOrdinal("ReferencedTable")));
                grouped.Add(name, fk);
            }

            fk.ParentColumns.Add(reader.GetString(reader.GetOrdinal("ParentColumnName")));
            fk.ReferencedColumns.Add(reader.GetString(reader.GetOrdinal("ReferencedColumnName")));
        }

        return grouped.Values
            .Select(static fk => new DbForeignKey(
                fk.Name,
                fk.ReferencedSchema,
                fk.ReferencedTable,
                fk.ParentColumns,
                fk.ReferencedColumns))
            .ToList();
    }

    private static async Task<List<DbProgrammableObject>> GetViewsAsync(SqlConnection connection)
    {
        const string sql =
            """
            SELECT
                s.name AS SchemaName,
                v.name AS ObjectName,
                m.definition AS Definition
            FROM sys.views v
            INNER JOIN sys.schemas s ON v.schema_id = s.schema_id
            LEFT JOIN sys.sql_modules m ON v.object_id = m.object_id
            WHERE v.is_ms_shipped = 0
            ORDER BY s.name, v.name;
            """;

        return await ReadProgrammableObjectsAsync(connection, sql, "VIEW");
    }

    private static async Task<List<DbProgrammableObject>> GetStoredProceduresAsync(SqlConnection connection)
    {
        const string sql =
            """
            SELECT
                s.name AS SchemaName,
                p.name AS ObjectName,
                m.definition AS Definition
            FROM sys.procedures p
            INNER JOIN sys.schemas s ON p.schema_id = s.schema_id
            LEFT JOIN sys.sql_modules m ON p.object_id = m.object_id
            WHERE p.is_ms_shipped = 0
            ORDER BY s.name, p.name;
            """;

        return await ReadProgrammableObjectsAsync(connection, sql, "PROCEDURE");
    }

    private static async Task<List<DbProgrammableObject>> GetFunctionsAsync(SqlConnection connection)
    {
        const string sql =
            """
            SELECT
                s.name AS SchemaName,
                o.name AS ObjectName,
                m.definition AS Definition
            FROM sys.objects o
            INNER JOIN sys.schemas s ON o.schema_id = s.schema_id
            LEFT JOIN sys.sql_modules m ON o.object_id = m.object_id
            WHERE o.is_ms_shipped = 0
              AND o.type IN ('FN', 'IF', 'TF', 'FS', 'FT', 'AF')
            ORDER BY s.name, o.name;
            """;

        return await ReadProgrammableObjectsAsync(connection, sql, "FUNCTION");
    }

    private static async Task<List<DbProgrammableObject>> ReadProgrammableObjectsAsync(SqlConnection connection, string sql, string objectType)
    {
        await using var cmd = new SqlCommand(sql, connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var objects = new List<DbProgrammableObject>();
        while (await reader.ReadAsync())
        {
            objects.Add(new DbProgrammableObject(
                ObjectType: objectType,
                Schema: reader.GetString(reader.GetOrdinal("SchemaName")),
                Name: reader.GetString(reader.GetOrdinal("ObjectName")),
                Definition: reader.IsDBNull(reader.GetOrdinal("Definition"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("Definition"))));
        }

        return objects;
    }

    private static async Task<List<DbUser>> GetUsersAsync(SqlConnection connection)
    {
        const string sql =
            """
            SELECT
                dp.name AS UserName,
                dp.type_desc AS UserType,
                dp.authentication_type_desc AS AuthenticationType,
                dp.default_schema_name AS DefaultSchema,
                sp.name AS LoginName
            FROM sys.database_principals dp
            LEFT JOIN sys.server_principals sp ON dp.sid = sp.sid
            WHERE dp.principal_id > 4
              AND dp.type IN ('S', 'U', 'G', 'E', 'X')
              AND dp.name NOT IN ('dbo', 'guest', 'INFORMATION_SCHEMA', 'sys')
            ORDER BY dp.name;
            """;

        await using var cmd = new SqlCommand(sql, connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var users = new List<DbUser>();
        while (await reader.ReadAsync())
        {
            users.Add(new DbUser(
                Name: reader.GetString(reader.GetOrdinal("UserName")),
                UserType: reader.GetString(reader.GetOrdinal("UserType")),
                AuthenticationType: reader.IsDBNull(reader.GetOrdinal("AuthenticationType"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("AuthenticationType")),
                DefaultSchema: reader.IsDBNull(reader.GetOrdinal("DefaultSchema"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("DefaultSchema")),
                LoginName: reader.IsDBNull(reader.GetOrdinal("LoginName"))
                    ? null
                    : reader.GetString(reader.GetOrdinal("LoginName"))));
        }

        return users;
    }

    private static void AppendCreateTable(StringBuilder sb, DbTable table, List<DbColumn> columns, List<string> primaryKey)
    {
        sb.AppendLine($"-- Table: {table.Schema}.{table.Name}");
        sb.AppendLine($"CREATE TABLE {SqlName(table.Schema, table.Name)} (");

        var definitionLines = new List<string>();
        foreach (var column in columns)
        {
            definitionLines.Add($"    {SqlName(column.Name)} {BuildColumnDefinition(column)}");
        }

        if (primaryKey.Count > 0)
        {
            definitionLines.Add($"    PRIMARY KEY ({string.Join(", ", primaryKey.Select(SqlName))})");
        }

        sb.AppendLine(string.Join(",\n", definitionLines));
        sb.AppendLine(");");
    }

    private static void AppendForeignKeys(StringBuilder sb, DbTable table, List<DbForeignKey> foreignKeys)
    {
        foreach (var fk in foreignKeys)
        {
            sb.AppendLine(
                $"ALTER TABLE {SqlName(table.Schema, table.Name)} ADD CONSTRAINT {SqlName(fk.Name)} FOREIGN KEY ({string.Join(", ", fk.ParentColumns.Select(SqlName))}) REFERENCES {SqlName(fk.ReferencedSchema, fk.ReferencedTable)} ({string.Join(", ", fk.ReferencedColumns.Select(SqlName))});");
        }
    }

    private static void AppendProgrammableObjectsSection(StringBuilder sb, string sectionName, List<DbProgrammableObject> objects)
    {
        sb.AppendLine($"-- ===== {sectionName} =====");
        if (objects.Count == 0)
        {
            sb.AppendLine("-- (none)");
            sb.AppendLine();
            return;
        }

        foreach (var item in objects)
        {
            sb.AppendLine($"-- {item.ObjectType}: {item.Schema}.{item.Name}");
            if (string.IsNullOrWhiteSpace(item.Definition))
            {
                sb.AppendLine($"-- Definition unavailable for {item.Schema}.{item.Name} (possibly encrypted).");
                sb.AppendLine();
                continue;
            }

            sb.AppendLine(item.Definition.Trim());
            sb.AppendLine("GO");
            sb.AppendLine();
        }
    }

    private static void AppendUsersSection(StringBuilder sb, List<DbUser> users)
    {
        sb.AppendLine("-- ===== Users =====");
        if (users.Count == 0)
        {
            sb.AppendLine("-- (none)");
            sb.AppendLine();
            return;
        }

        foreach (var user in users)
        {
            sb.AppendLine($"-- UserType={user.UserType}; AuthenticationType={user.AuthenticationType ?? "UNKNOWN"}");
            sb.AppendLine(BuildCreateUserStatement(user));
            sb.AppendLine("GO");
            sb.AppendLine();
        }
    }

    private static string BuildCreateUserStatement(DbUser user)
    {
        var sb = new StringBuilder();
        sb.Append($"CREATE USER {SqlName(user.Name)}");

        if (!string.IsNullOrWhiteSpace(user.LoginName))
        {
            sb.Append($" FOR LOGIN {SqlName(user.LoginName)}");
        }
        else if (string.Equals(user.AuthenticationType, "EXTERNAL", StringComparison.OrdinalIgnoreCase))
        {
            sb.Append(" FROM EXTERNAL PROVIDER");
        }
        else if (string.Equals(user.AuthenticationType, "DATABASE", StringComparison.OrdinalIgnoreCase))
        {
            sb.Append(" WITHOUT LOGIN");
        }

        if (!string.IsNullOrWhiteSpace(user.DefaultSchema))
        {
            sb.Append($" WITH DEFAULT_SCHEMA = {SqlName(user.DefaultSchema)}");
        }

        sb.Append(";");
        return sb.ToString();
    }

    private async Task AppendSampleDataAsync(StringBuilder sb, SqlConnection connection, DbTable table, List<DbColumn> columns)
    {
        var selectedColumns = columns.Where(static c => !c.IsComputed).ToList();
        if (selectedColumns.Count == 0)
        {
            return;
        }

        sb.AppendLine();
        sb.AppendLine($"-- Sample data for {table.Schema}.{table.Name} (TOP {_options.TopRows})");
        sb.AppendLine("-- Context-only sample rows for AI prompts.");

        var query = $"SELECT TOP ({_options.TopRows}) {string.Join(", ", selectedColumns.Select(c => SqlName(c.Name)))} FROM {SqlName(table.Schema, table.Name)} ORDER BY (SELECT 1);";
        await using var cmd = new SqlCommand(query, connection);
        await using var reader = await cmd.ExecuteReaderAsync();

        var wroteAnyRows = false;
        while (await reader.ReadAsync())
        {
            wroteAnyRows = true;
            var values = new List<string>(selectedColumns.Count);
            for (var i = 0; i < selectedColumns.Count; i++)
            {
                values.Add(ToSqlLiteral(reader.IsDBNull(i) ? null : reader.GetValue(i)));
            }

            sb.AppendLine(
                $"INSERT INTO {SqlName(table.Schema, table.Name)} ({string.Join(", ", selectedColumns.Select(c => SqlName(c.Name)))}) VALUES ({string.Join(", ", values)});");
        }

        if (!wroteAnyRows)
        {
            sb.AppendLine("-- (no rows)");
        }
    }

    private static string BuildColumnDefinition(DbColumn column)
    {
        if (column.IsComputed)
        {
            var persisted = column.IsPersistedComputed ? " PERSISTED" : string.Empty;
            return $"AS {column.ComputedDefinition}{persisted}";
        }

        var sb = new StringBuilder();
        sb.Append(BuildDataType(column));

        if (column.IsIdentity)
        {
            sb.Append($" IDENTITY({column.IdentitySeed}, {column.IdentityIncrement})");
        }

        if (!string.IsNullOrWhiteSpace(column.DefaultDefinition))
        {
            sb.Append($" DEFAULT {column.DefaultDefinition}");
        }

        sb.Append(column.IsNullable ? " NULL" : " NOT NULL");
        return sb.ToString();
    }

    private static string BuildDataType(DbColumn column)
    {
        var type = column.DataType;

        return type switch
        {
            "varchar" or "char" or "varbinary" or "binary" => $"{type}({LengthLiteral(column.MaxLength)})",
            "nvarchar" or "nchar" => $"{type}({LengthLiteral(column.MaxLength / 2)})",
            "decimal" or "numeric" => $"{type}({column.Precision}, {column.Scale})",
            "datetime2" or "datetimeoffset" or "time" => $"{type}({column.Scale})",
            _ => type
        };
    }

    private static string LengthLiteral(int length) => length == -1 ? "max" : length.ToString(CultureInfo.InvariantCulture);

    private static string SqlName(string identifier) => $"[{identifier.Replace("]", "]]", StringComparison.Ordinal)}]";

    private static string SqlName(string schema, string table) => $"{SqlName(schema)}.{SqlName(table)}";

    private static string ToSqlLiteral(object? value)
    {
        if (value is null or DBNull)
        {
            return "NULL";
        }

        return value switch
        {
            string s => $"N'{EscapeSqlString(s)}'",
            char ch => $"N'{EscapeSqlString(ch.ToString())}'",
            bool b => b ? "1" : "0",
            byte or sbyte or short or ushort or int or uint or long or ulong => Convert.ToString(value, CultureInfo.InvariantCulture)!,
            float f => f.ToString("R", CultureInfo.InvariantCulture),
            double d => d.ToString("R", CultureInfo.InvariantCulture),
            decimal m => m.ToString(CultureInfo.InvariantCulture),
            Guid g => $"'{g}'",
            DateTime dt => $"'{dt:yyyy-MM-dd HH:mm:ss.fffffff}'",
            DateTimeOffset dto => $"'{dto:yyyy-MM-dd HH:mm:ss.fffffff zzz}'",
            TimeSpan ts => $"'{ts:c}'",
            byte[] bytes => $"0x{Convert.ToHexString(bytes)}",
            _ => $"N'{EscapeSqlString(Convert.ToString(value, CultureInfo.InvariantCulture) ?? string.Empty)}'"
        };
    }

    private static string EscapeSqlString(string input) => input.Replace("'", "''", StringComparison.Ordinal);

    private sealed class DbForeignKeyBuilder
    {
        public DbForeignKeyBuilder(string name, string referencedSchema, string referencedTable)
        {
            Name = name;
            ReferencedSchema = referencedSchema;
            ReferencedTable = referencedTable;
        }

        public string Name { get; }
        public string ReferencedSchema { get; }
        public string ReferencedTable { get; }
        public List<string> ParentColumns { get; } = new();
        public List<string> ReferencedColumns { get; } = new();
    }
}

internal sealed record DbTable(string Schema, string Name);

internal sealed record DbColumn(
    int Ordinal,
    string Name,
    string DataType,
    short MaxLength,
    byte Precision,
    byte Scale,
    bool IsNullable,
    bool IsIdentity,
    string IdentitySeed,
    string IdentityIncrement,
    bool IsComputed,
    string? ComputedDefinition,
    bool IsPersistedComputed,
    string? DefaultDefinition);

internal sealed record DbForeignKey(
    string Name,
    string ReferencedSchema,
    string ReferencedTable,
    IReadOnlyList<string> ParentColumns,
    IReadOnlyList<string> ReferencedColumns);

internal sealed record DbProgrammableObject(
    string ObjectType,
    string Schema,
    string Name,
    string? Definition);

internal sealed record DbUser(
    string Name,
    string UserType,
    string? AuthenticationType,
    string? DefaultSchema,
    string? LoginName);
