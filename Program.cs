using System.Globalization;
using System.Text;
using Microsoft.Data.SqlClient;

var options = CliOptions.Parse(args);
if (options.ShowHelp)
{
    CliOptions.PrintHelp();
    return;
}

if (string.IsNullOrWhiteSpace(options.ConnectionString))
{
    Console.Error.WriteLine("Missing connection string. Use --connection or SQLSYNC_CONNECTION_STRING.");
    CliOptions.PrintHelp();
    Environment.ExitCode = 1;
    return;
}

try
{
    var generator = new ContextScriptGenerator(options);
    await generator.GenerateAsync();
    Console.WriteLine($"Wrote context script to {options.OutputPath}");
}
catch (Exception ex)
{
    Console.Error.WriteLine($"Failed: {ex.Message}");
    Environment.ExitCode = 1;
}

internal sealed record CliOptions(
    string ConnectionString,
    string OutputPath,
    bool IncludeData,
    int TopRows,
    IReadOnlySet<string>? TableFilter,
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

                default:
                    throw new ArgumentException($"Unknown option: {arg}");
            }
        }

        return new CliOptions(connection, output, includeData, topRows, tableFilter, showHelp);
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
              -o, --output <path>         Output .sql path (default: sqlsync-context.sql)
              -t, --top <n>               Top N sample rows per table (default: 10)
                  --schema-only           Export schema only
                  --include-data          Export schema + sample data (default)
                  --tables <list>         Comma-separated table names (dbo.Users,sales.Orders)
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
            .Select(static name => name.Contains('.') ? name : $"dbo.{name}");

        return new HashSet<string>(values, StringComparer.OrdinalIgnoreCase);
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
