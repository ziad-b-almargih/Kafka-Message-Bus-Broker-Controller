using Confluent.Kafka;


var builder = WebApplication.CreateBuilder(args);
var app = builder.Build();

app.UseDefaultFiles();
app.UseStaticFiles();

var config = new ProducerConfig { BootstrapServers = Environment.GetEnvironmentVariable("KAFKA_SERVER") ?? "localhost:9092" };
var producer = new ProducerBuilder<Null, string>(config).Build();

app.MapPost("/api/send", async (MessagePayload request) =>
{
    var message = new Message<Null, string> { Value = request.payload };
    await producer.ProduceAsync("warehouse-processing-lane", message);
    return Results.Ok(new { status = "Sent", request.payload });
});

app.Run();

public class MessagePayload { public required string payload { get; set; } }