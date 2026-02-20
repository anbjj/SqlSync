# SqlSync

SqlSync is a `.NET 10` console app that connects to SQL Server and exports database context to a `.sql` file:
- Table schema (`CREATE TABLE`)
- Primary keys and foreign keys
- Optional sample data (`TOP N` rows per table as `INSERT` statements)

## Usage

```bash
SqlSync --connection "<SQL Server connection string>" --output sqlsync-context.sql --top 10
```

Options:
- `-c, --connection <value>`: SQL Server connection string
- `--connection-name <n>`: Load connection string from `connstr/<n>.txt`
- `--save-connection <n>`: Save resolved connection string to `connstr/<n>.txt`
- `-o, --output <path>`: Output `.sql` file path (default `sqlsync-context.sql`)
- `-t, --top <n>`: Top rows per table (default `10`)
- `--schema-only`: Export schema only
- `--include-data`: Export schema + sample data (default)
- `--tables <list>`: Comma-separated table list, e.g. `dbo.Users,sales.Orders`
- `-h, --help`: Show help

You can also provide the connection string via environment variable:

```bash
export SQLSYNC_CONNECTION_STRING="<SQL Server connection string>"
SqlSync --output sqlsync-context.sql
```

Store/load profile examples (kept out of git):

```bash
SqlSync --connection "<SQL Server connection string>" --save-connection dev
SqlSync --connection-name dev --output sqlsync-context.sql
```

## Build and run

```bash
dotnet restore
dotnet run -- --connection "<SQL Server connection string>"
```
