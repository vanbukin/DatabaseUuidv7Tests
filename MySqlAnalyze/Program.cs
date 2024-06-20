using System.Text;
using MySqlConnector;

namespace MySqlAnalyze;

record DbRecord(byte[] Value, long ReadOrder, long WriteOrder, long Diff);

class Bucket
{
    public Bucket(int fromInclusive, int toExclusive)
    {
        FromInclusive = fromInclusive;
        ToExclusive = toExclusive;
    }

    public int FromInclusive { get; }
    public int ToExclusive { get; }
    private int _items;
    public int Items => _items;

    public void IncrementItems()
    {
        _items++;
    }
}

public static class Program
{
    private const string ConnectionString =
        "Server=localhost;Port=3306;User ID=root;Password=root;Database=dotnet";

    private const int BucketSize = 100_000;

    public static async Task Main(string[] args)
    {
        var allRecords = await ReadAllAsync();
        var buckets = GetBuckets(allRecords, BucketSize);
        foreach (var dbRecord in allRecords)
        {
            foreach (var bucket in buckets)
            {
                if (dbRecord.Diff >= bucket.FromInclusive && dbRecord.Diff < bucket.ToExclusive)
                {
                    bucket.IncrementItems();
                    break;
                }
            }
        }

        foreach (var line in buckets.Select((x, i) => $"{i * BucketSize:D}\t{x.Items:D}"))
        {
            Console.WriteLine(line);
        }

        Console.WriteLine("------------------------------------------------");
        Console.WriteLine($"Max deviation: {allRecords.Max(x => x.Diff)}");
        Console.WriteLine("------------------------------------------------");
        Console.WriteLine("DONE");
    }

    private static Bucket[] GetBuckets(List<DbRecord> records, int bucketSize)
    {
        var max = records.Max(x => x.Diff);
        var bucketsCount = Math.DivRem(max, bucketSize, out var remainder);
        if (remainder > 0)
        {
            bucketsCount++;
        }

        var buckets = new List<Bucket> { new(0, 1) };
        for (int i = 0; i < bucketsCount; i++)
        {
            var fromInclusive = i * bucketSize + 1;
            var toExclusive = (i + 1) * bucketSize + 1;
            var bucket = new Bucket(fromInclusive, toExclusive);
            buckets.Add(bucket);
        }

        return buckets.ToArray();
    }

    static async Task<List<DbRecord>> ReadAllAsync()
    {
        var result = new List<DbRecord>(10_000_000);
        await using var connection = new MySqlConnection(ConnectionString);
        await connection.OpenAsync();
        await using var cmd = connection.CreateCommand();
        cmd.CommandText = "SELECT `uuid`, `order` FROM uuids ORDER BY uuid ASC;";
        await using var reader = await cmd.ExecuteReaderAsync();
        var readOrder = 0L;
        while (await reader.ReadAsync())
        {
            if (reader[0] is not byte[] data || reader[1] is not long writeOrder)
            {
                throw new InvalidOperationException();
            }

            var dbRecord = new DbRecord(data, readOrder, writeOrder, Math.Abs(readOrder - writeOrder));
            readOrder++;
            result.Add(dbRecord);
        }

        return result;
    }
}