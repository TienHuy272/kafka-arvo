package hnt.kafka.arvo.producer;

import hnt.kafka.arvo.dto.Employee;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import java.util.concurrent.CompletableFuture;

@Service
public class KafkaProducer {

    @Value("${topic.name}")
    private String topicName;

    @Autowired
    private KafkaTemplate<String, Employee> kafkaTemplate;

    public void send(Employee employee) {
        CompletableFuture<SendResult<String, Employee>> future = kafkaTemplate.send(topicName, employee);
        future.whenComplete((result, ex) -> {
            if (ex == null) {
                System.out.println("Sent message=[" + employee +
                        "] with offset=[" + result.getRecordMetadata().offset() + "]");
            } else {
                System.out.println("Unable to send message=[" +
                        employee + "] due to : " + ex.getMessage());
            }
        });
    }

}
