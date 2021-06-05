package in.yerqueri.producer;

import in.yerqueri.producer.publisher.DemoProducer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.http.ResponseEntity;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;
import reactor.kafka.sender.SenderRecord;

import java.util.UUID;

@SpringBootApplication
@RestController
@EnableKafka
@Slf4j
public class ProducerApplication {

    public static void main(String[] args) {
        SpringApplication.run(ProducerApplication.class, args);
    }

    @Autowired
    private DemoProducer producer;

    @RequestMapping(value = "/sendMessage", method = RequestMethod.GET)
    public ResponseEntity recieveRequest() {
        for(int i=0;i<1000;i++) {
            Flux<SenderRecord<String, String, String>> outboundFlux = Flux.just(i)
                    .map(record -> producer.generateSenderRecord("Key" + UUID.randomUUID(), convertPojoToJson(record), "Transaction_" + UUID.randomUUID(), DemoProducer.TOPIC));
            producer.sendMessage(outboundFlux);
        }
        return ResponseEntity.ok("THANKS");
    }

    private String convertPojoToJson(int i) {
        return "[ "+i+" ]";
    }

}
