using System.Buffers.Binary;
using System.Data;
using System.Diagnostics;
using System.Security.Cryptography;
using System.Text;
using System.Text.Encodings.Web;
using System.Text.Json;
using System.Text.Json.Serialization;
using MySqlConnector;

namespace MySqlTest;

record Stats(
    int RunNumber,
    DateTimeOffset CreatedAt,
    long InsertedInIteration,
    long InsertedTotal,
    TimeSpan InsertDuration,
    TimeSpan SelectDuration);

public static class Program
{
    private const string ConnectionString =
        "Server=localhost;Port=3306;User ID=root;Password=root;Database=dotnet;GUID Format=Binary16";

    private const int NumberOfRuns = 15;
    private const int RunSize = 1_000_000;
    private const int TransactionChunkSize = 100_000;
    private const int SingleInsertChunkSize = 100_000;

    static async Task Main(string[] args)
    {
        var alreadyInserted = await ReadAllAsync();
        for (int i = 1; i <= NumberOfRuns; i++)
        {
            var uuids = Enumerable.Range(0, RunSize).Select(_ => new Guid(GenerateUuidV7()));
            var swUpload = Stopwatch.StartNew();
            var uploadedItems =
                await UploadUuidsAsync(uuids, TransactionChunkSize, SingleInsertChunkSize, alreadyInserted);
            swUpload.Stop();
            var swRead = Stopwatch.StartNew();
            alreadyInserted = await ReadAllAsync();
            swRead.Stop();
            var stats = new Stats(i,
                DateTimeOffset.UtcNow,
                uploadedItems,
                alreadyInserted,
                swUpload.Elapsed,
                swRead.Elapsed);
            PrintStats(stats);
        }

        Console.WriteLine("------------------------------------------------");
        Console.WriteLine("DONE");
    }

    static void PrintStats(Stats stats)
    {
        var json = JsonSerializer.Serialize(stats, new JsonSerializerOptions(JsonSerializerDefaults.General)
        {
            Encoder = JavaScriptEncoder.UnsafeRelaxedJsonEscaping,
            DefaultIgnoreCondition = JsonIgnoreCondition.Never,
            WriteIndented = true
        });
        Console.WriteLine("------------------------------------------------");
        Console.WriteLine(json);
    }

    static async Task<long> ReadAllAsync()
    {
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT `uuid`, `order` FROM uuids ORDER BY uuid ASC;";
        await using var reader = await cmd.ExecuteReaderAsync();
        var recordsRead = 0L;
        while (await reader.ReadAsync())
        {
            recordsRead++;
        }

        return recordsRead;
    }

    static async Task<long> UploadUuidsAsync(
        IEnumerable<Guid> uuids,
        int transactionChunkSize,
        int singleInsertChunkSize,
        long alreadyInserted)
    {
        var result = 0L;
        var totalInserted = alreadyInserted;
        var transactionsChunks = uuids.Chunk(transactionChunkSize);
        foreach (var transactionChunk in transactionsChunks)
        {
            var inserted = await UploadUuidsTransactionAsync(transactionChunk, singleInsertChunkSize, totalInserted);
            result += inserted;
            totalInserted += inserted;
        }

        return result;
    }

    static async Task<long> UploadUuidsTransactionAsync(Guid[] uuids, int singleInsertChunkSize, long alreadyInserted)
    {
        if (uuids.Length == 0)
        {
            return 0;
        }

        var insertChunks = uuids.Chunk(singleInsertChunkSize);
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var transaction = await connection.BeginTransactionAsync(IsolationLevel.ReadCommitted);
        var result = 0L;
        var totalInserted = alreadyInserted;
        foreach (var insertChunk in insertChunks)
        {
            var inserted = await UploadUuidsChunkAsync(insertChunk, totalInserted, connection, transaction);
            result += inserted;
            totalInserted += inserted;
        }

        await transaction.CommitAsync();

        return result;
    }

    static async Task<long> UploadUuidsChunkAsync(
        Guid[] uuids,
        long alreadyInserted,
        MySqlConnection connection,
        MySqlTransaction transaction)
    {
        if (uuids.Length == 0)
        {
            return 0;
        }

        var requestBuilder = new StringBuilder("INSERT INTO `uuids` (`uuid`, `order`) VALUES ");
        var firstRecord = true;
        for (int i = 0; i < uuids.Length; i++)
        {
         
            if (firstRecord)
            {
                requestBuilder.Append($"(@u{i},@o{i})");
                firstRecord = false;
            }
            else
            {
                requestBuilder.Append($",(@u{i},@o{i})");
            }
        }

        requestBuilder.Append(';');
        var sql = requestBuilder.ToString();
        await using var cmd = connection.CreateCommand();
        cmd.Transaction = transaction;
        cmd.CommandText = sql;
        for (int i = 0; i < uuids.Length; i++)
        {
            var uuid = uuids[i];
            var order = alreadyInserted + i;
            cmd.Parameters.AddWithValue($"@u{i}", uuid);
            cmd.Parameters.AddWithValue($"@o{i}", order);
        }

        await cmd.ExecuteNonQueryAsync();
        return uuids.Length;
    }

    static string GenerateUuidV7()
    {
        Span<byte> uuidv7 = stackalloc byte[16];
        ulong unixTimeTicks = (ulong)DateTimeOffset.UtcNow.Subtract(DateTimeOffset.UnixEpoch).Ticks;
        ulong unixTsMs = (unixTimeTicks & 0x0FFFFFFFFFFFF000) << 4;
        ulong unixTsMsVer = unixTsMs | 0b0111UL << 12;
        ulong randA = unixTimeTicks & 0x0000000000000FFF;
        // merge "unix_ts_ms", "ver" and "rand_a"
        ulong hi = unixTsMsVer | randA;
        BinaryPrimitives.WriteUInt64BigEndian(uuidv7, hi);
        // fill "rand_b" and "var"
        RandomNumberGenerator.Fill(uuidv7[8..]);
        // set "var"
        byte varOctet = uuidv7[8];
        varOctet = (byte)(varOctet & 0b00111111);
        varOctet = (byte)(varOctet | 0b10111111);
        uuidv7[8] = varOctet;
        return Convert.ToHexString(uuidv7);
    }
}