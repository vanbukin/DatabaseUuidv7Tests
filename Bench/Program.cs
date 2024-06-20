using System.Buffers.Binary;
using System.Security.Cryptography;
using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using BenchmarkDotNet.Running;

namespace Bench;

class Program
{
    static void Main(string[] args)
    {
        BenchmarkRunner.Run<UuidGenerator>();
    }
}

[GcServer(true)]
[MemoryDiagnoser]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class UuidGenerator
{
    [Benchmark]
    public string GenerateUuidV7()
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