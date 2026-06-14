using Confluent.Kafka;
using System.Collections.Concurrent;

var builder = WebApplication.CreateBuilder(args);
builder.Services.AddHostedService<WorkerService>();
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

var baseUrl = Environment.GetEnvironmentVariable("BASE_URL") ?? "";
app.MapGet("/config.js", () =>
    Results.Content($"window.BASE_URL = '{baseUrl}';", "application/javascript"));

app.MapGet("/api/logs", () => Results.Ok(WorkerService.Logs));

app.Run();

public class WorkerService : BackgroundService
{
    public static readonly ConcurrentQueue<string> Logs = new(); 

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        await Task.Yield();
        var kafkaServer = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? "localhost:9092";
        
        var consumerConfig = new ConsumerConfig { BootstrapServers = kafkaServer, GroupId = "worker-group", EnableAutoCommit = false };
        var producerConfig = new ProducerConfig { BootstrapServers = kafkaServer };

        using var consumer = new ConsumerBuilder<Ignore, string>(consumerConfig).Build();
        using var producer = new ProducerBuilder<Null, string>(producerConfig).Build();
        
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                consumer.Subscribe("warehouse-processing-lane");
                AddLog("Worker Subscribed. Waiting for messages...");
                break;
            }
            catch (KafkaException ex)
            {
                AddLog($"[Kafka Init Error] Broker/Topic not ready. Retrying in 5s... Ex: {ex.Message}");
                await Task.Delay(5000, stoppingToken);
            }
        }
        
        AddLog("Worker Started. Waiting for messages...");

        while (!stoppingToken.IsCancellationRequested)
        {
            ConsumeResult<Ignore, string>? result;
            try
            {
                result = consumer.Consume(stoppingToken);
            }
            catch (OperationCanceledException)
            {
                break;
            }
            catch (ConsumeException ex) when (ex.Error.Code == ErrorCode.UnknownTopicOrPart)
            {
                AddLog("[Topic Error] Main topic not available on broker. Retrying in 5s...");
                await Task.Delay(5000, stoppingToken);
                continue;
            }
            catch (ConsumeException ex)
            {
                AddLog($"[Consume Error] {ex.Error.Reason}. Retrying in 3s...");
                await Task.Delay(3000, stoppingToken);
                continue;
            }

            if (result == null) continue;

            var payload = result.Message.Value;
            var retries = 0;
            var success = false;

            while (retries < 3 && !success)
            {
                try
                {
                    AddLog($"Processing {payload}...");
                    if (payload.Contains(":fail")) throw new Exception("Simulated Crash");
                    success = true;
                    AddLog($"[Success] {payload} processed.");
                }
                catch (Exception ex)
                {
                    retries++;
                    AddLog($"[Error] Failed {payload}. Attempt {retries}/3, Ex: {ex.Message}");
                    if (retries < 3) await Task.Delay(1000, stoppingToken);
                }
            }

            if (!success)
            {
                AddLog($"[DLQ Route] Isolating {payload} to DLQ.");
                var dlqSent = false;
                
                while (!dlqSent && !stoppingToken.IsCancellationRequested)
                {
                    try
                    {
                        await producer.ProduceAsync("warehouse-dead-letter-queue", new Message<Null, string> { Value = payload }, stoppingToken);
                        dlqSent = true;
                    }
                    catch (Exception ex)
                    {
                        AddLog($"[DLQ Critical] Failed to push to DLQ topic! Retrying in 3s... Error: {ex.Message}");
                        await Task.Delay(3000, stoppingToken);
                    }
                }
            }
            try
            {
                consumer.Commit(result);
            }
            catch (Exception ex)
            {
                AddLog($"[Commit Error] Failed to commit offset: {ex.Message}");
            }
        }
        try { consumer.Close(); }
        catch
        {
            // ignored
        }
    }

    private static void AddLog(string msg)
    {
        Logs.Enqueue($"[{DateTime.Now:HH:mm:ss}] {msg}");
        if (Logs.Count > 50) Logs.TryDequeue(out _); // الاحتفاظ بآخر 50 رسالة فقط
    }
}