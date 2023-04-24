import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;
import reactor.kafka.sender.KafkaSender;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;

public class ReactorKafka341 {
    public static void main(final String[] args) {

        while (true) {
            final KafkaSender<Integer, String> sender = sender();

            final Flux<SenderRecord<Integer, String, String>> flux = Flux
                    .just(((int)(10000*Math.random()))+"")
                    .map(event -> SenderRecord.<Integer, String, String>create("topic", null, null, null, event, event))
                    .publishOn(Schedulers.parallel());
                    ;

            sender
                    .send(flux)
                    .doFinally(signalType -> sender.close())
                    .blockLast(Duration.ofSeconds(30))
            ;
        }
    }

    private static KafkaSender<Integer, String> sender() {
        final Map<String, Object> properties = new HashMap<>();

        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class);
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class);
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");

        final SenderOptions<Integer, String> opts = SenderOptions.create(properties);
        opts.stopOnError(false);

        return KafkaSender.create(opts);
    }
}
