using BenchmarkDotNet.Attributes;
using Microsoft.Data.SqlClient;
using Microsoft.IdentityModel.Tokens;

namespace SqlReadAsync;

[SimpleJob(1, 1, 1, 1, "1", false)]
public class Benchmarks
{
	private const string SERVER = "server";
	private const string DATABASE = "db";
	private const string USER = "sa";
	private const string PASSWORD = "password";
	private const string CONNECTION_STRING = $"Data Source={SERVER};Initial Catalog={DATABASE};User ID={USER};Password={PASSWORD};Trust Server Certificate=True;Pooling=False;";
	
	#region TABLE SQL

	//DROP TABLE IF EXISTS dbo.test_table;
	//WITH t1(n) AS
	//	(
	//		SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL
	//			UNION ALL SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL UNION ALL SELECT NULL
	//	)
	//	, t2(n) AS
	//(
	//	SELECT NULL FROM t1 AS t1, t1 AS t2, t1 AS t3, t1 AS t4, t1 AS t5, t1 AS t6, t1 AS t7, t1 AS t8, t1 AS t9
	//)
	//, t3(n) AS
	//(
	//	SELECT TOP 50000000 ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) FROM t2
	//)
	//SELECT
	//	ID = ISNULL(t.n, 0),
	//	c1 = CAST(t.n AS VARCHAR(12)),
	//	d1 = DATEADD(MINUTE, t.n, '1970-01-01')
	//INTO dbo.test_table FROM t3 AS t;
	//ALTER TABLE dbo.test_table ADD CONSTRAINT PK_test_table PRIMARY KEY(ID);

	#endregion

	private const string SELECT_PART = "SELECT ID, c1, d1 FROM dbo.test_table";
	private readonly string[] whereParts =
	[
		" WHERE ID > 0 AND ID <= 10000000",
		" WHERE ID > 10000000 AND ID <= 20000000",
		" WHERE ID > 20000000 AND ID <= 30000000",
		" WHERE ID > 30000000 AND ID <= 40000000",
		" WHERE ID > 40000000 AND ID <= 50000000",
	];

	public enum ReadType
	{
		Sync,
		Async,
		AsyncToken,
		AsyncTokenSource,
	}

	[Params(ReadType.Sync, ReadType.Async, ReadType.AsyncToken, ReadType.AsyncTokenSource)]
	public ReadType Type { get; set; }
	
	private static Task ReadSync(string? whereClause)
	{
		using var conn = new SqlConnection(CONNECTION_STRING);
		using var cmd = new SqlCommand($"{SELECT_PART}{whereClause}", conn);
		conn.Open();
		using SqlDataReader reader = cmd.ExecuteReader();

		while (reader.Read()) { }

		return Task.CompletedTask;
	}

	private static async Task ReadAsync(string whereClause, CancellationToken cancellation)
	{
		using var conn = new SqlConnection(CONNECTION_STRING);
		using var cmd = new SqlCommand($"{SELECT_PART}{whereClause}", conn) { CommandTimeout = 0 };
		conn.Open();
		using SqlDataReader reader = cmd.ExecuteReader();

		while (await reader.ReadAsync(cancellation).ConfigureAwait(false)) { }
	}

	[Benchmark]
	public async Task TestRead()
	{
		CancellationToken cancellation = CancellationToken.None;
		switch (Type)
		{
			case ReadType.Sync:
			case ReadType.Async:
				cancellation = CancellationToken.None;
				break;
			case ReadType.AsyncToken:
				cancellation = new CancellationToken();
				break;
			case ReadType.AsyncTokenSource:
				cancellation = new CancellationTokenSource().Token;
				break;
		}

		var tasks = new List<Task>();

		foreach (string wherePart in whereParts)
		{
			switch (Type)
			{
				case ReadType.Sync:
					tasks.Add(Task.Run(() => ReadSync(wherePart)));
					break;
				case ReadType.Async:
					tasks.Add(Task.Run(() => ReadAsync(wherePart, cancellation)));
					break;
				case ReadType.AsyncToken:
					tasks.Add(Task.Run(() => ReadAsync(wherePart, cancellation)));
					break;
				case ReadType.AsyncTokenSource:
					tasks.Add(Task.Run(() => ReadAsync(wherePart, cancellation)));
					break;
			}
		}

		await Task.WhenAll(tasks);
	}

	#region Cancellation

	//[Benchmark]
	public async Task TestCancellation()
	{
		CancellationToken cancellation = CancellationToken.None;
		switch (Type)
		{
			case ReadType.Sync:
			case ReadType.Async:
				cancellation = CancellationToken.None;
				break;
			case ReadType.AsyncToken:
				cancellation = new CancellationToken();
				break;
			case ReadType.AsyncTokenSource:
				cancellation = new CancellationTokenSource().Token;
				break;
		}

		var tasks = new List<Task>();

		foreach (string wherePart in whereParts)
		{
			switch (Type)
			{
				case ReadType.Sync:
					tasks.Add(Task.Run(() => Do(cancellation)));
					break;
				case ReadType.Async:
					tasks.Add(Task.Run(() => Do(cancellation)));
					break;
				case ReadType.AsyncToken:
					tasks.Add(Task.Run(() => Do(cancellation)));
					break;
				case ReadType.AsyncTokenSource:
					tasks.Add(Task.Run(() => Do(cancellation)));
					break;
			}
		}

		await Task.WhenAll(tasks);

		static void Do(CancellationToken cancellation)
		{
			for (long i = 0; i < 1_000_000_000; i++)
			{
				if (cancellation.IsCancellationRequested) break;
			}
		}
	}

	#endregion
}
