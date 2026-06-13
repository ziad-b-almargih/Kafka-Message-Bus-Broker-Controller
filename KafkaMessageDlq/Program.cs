using Confluent.Kafka;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<DlqService>();
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

app.MapGet("/api/dlq", () => Results.Ok(DlqService.DeadLetters));

app.Run();

public class DlqService : BackgroundService
{
    public static readonly ConcurrentQueue<string> DeadLetters = new();

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        var kafkaServer = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? "localhost:9092";
        var consumerConfig = new ConsumerConfig { BootstrapServers = kafkaServer, GroupId = "dlq-group", AutoOffsetReset = AutoOffsetReset.Earliest };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                consumer.Subscribe("warehouse-dead-letter-queue");
                Console.WriteLine("[DLQ Service] Subscribed successfully to topic.");
                break;
            }
            catch (KafkaException ex)
            {
                Console.WriteLine($"[DLQ Service] Kafka Broker or Topic connection failed. Retrying in 5s... Error: {ex.Message}");
                await Task.Delay(5000, stoppingToken);
            }
        }
        
        try 
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    var result = consumer.Consume(stoppingToken);
                    DeadLetters.Enqueue($"[{DateTime.Now:HH:mm:ss}] Poison Payload Isolated: {result.Message.Value}");
                }
                catch (OperationCanceledException)
                {
                    break; 
                }
                catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
                {
                    Console.WriteLine($"[DLQ Service] Topic not available yet. Retrying consume in 5s...");
                    await Task.Delay(5000, stoppingToken);
                }
                catch (ConsumeException ex)
                {
                    Console.WriteLine($"[DLQ Service] Consume error: {ex.Error.Reason}. Retrying in 3s...");
                    await Task.Delay(3000, stoppingToken);
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"[DLQ Service] Unexpected error: {ex.Message}");
                    await Task.Delay(2000, stoppingToken);
                } 
            }
        }
        finally 
        { 
            try { consumer.Close(); } catch { } 
        }

    }
}