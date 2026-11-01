package org.example.kafka.Controllers;

import org.example.kafka.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Date;
import java.util.Random;

@RestController
public class PageEventController {
    @Autowired
    private StreamBridge streamBridge; //permet d'envoyer les message de l'application spring  a n'importe quel broker
    @GetMapping("/publish")
    public PageEvent publish(String namepage, String topic){

        PageEvent pageEvent = new PageEvent(
                namepage,
                Math.random()<0.5?"U1":"U2",
                new Date(),
                10+new Random().nextInt(1000));
        streamBridge.send(topic,pageEvent);
        return  pageEvent;

    }
}
