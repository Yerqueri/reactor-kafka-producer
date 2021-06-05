package in.yerqueri.producer.publisher;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.StringSerializer;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderOptions;
import reactor.kafka.sender.SenderRecord;


@Slf4j
@Component
public class DemoProducer extends KafkaProducerUtil<String,String,String>{

    private static final String BOOTSTRAP_SERVERS = "localhost:9092";
    public static final String TOPIC = "demo-topic";

    @Override
    public SenderOptions<String, String> generateSenderOptions() {
        SenderOptions<String,String> senderOptions = SenderOptions.create();
        return senderOptions.producerProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,BOOTSTRAP_SERVERS)
                .producerProperty(ProducerConfig.CLIENT_ID_CONFIG, "sample-producer")
                .producerProperty(ProducerConfig.ACKS_CONFIG, "all")
                .withValueSerializer(new StringSerializer())
                .withKeySerializer(new StringSerializer());
    }

    @Override
    public void sendMessage(Flux<SenderRecord<String, String, String>> outboundFlux) {
        try {
            sender.send(outboundFlux)
                    //.doOnNext(result -> handleOnNext(result, eventRecord, errorHandlerCallback))
                    .doOnError(e -> log.error("->", e))
                    .parallel(3)
                    .subscribe(r -> log.info("-> {}", r));
        } catch (Exception ex) {
            log.error("Error Sending/Constructing Producer/Data: {}",ex);

        }
    }
}
