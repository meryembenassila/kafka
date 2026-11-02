package org.example.kafka.Controllers;

import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.KeyValueIterator;
import org.apache.kafka.streams.state.QueryableStoreTypes;
import org.apache.kafka.streams.state.ReadOnlyWindowStore;
import org.example.kafka.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.cloud.stream.function.StreamBridge;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;
import reactor.core.publisher.Flux;

import java.time.Duration;
import java.time.Instant;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

@RestController
public class PageEventController {
    @Autowired
    private StreamBridge streamBridge; //permet d'envoyer les message de l'application spring  a n'importe quel broker

    @Autowired
    InteractiveQueryService interactiveQueryService ;

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


    @GetMapping(path = "/analytics",produces = MediaType.TEXT_EVENT_STREAM_VALUE)
    public Flux<Map<String, Long>> analytics(){
        return Flux.interval(Duration.ofSeconds(1))//Flux.interval() crée un flux qui émet un numéro de séquence à intervalles réguliers.
                .map(sequence->{
                    Map<String,Long> stringLongMap=new HashMap<>();
                    ReadOnlyWindowStore<String, Long> windowStore = interactiveQueryService.getQueryableStore("count-store", QueryableStoreTypes.windowStore());//ReadOnlyWindowStore permet de consulter ces résultats sans les modifier.
                    Instant now=Instant.now();//now : l'instant actuel.
                    Instant from=now.minusMillis(5000);//from : l'instant actuel moins 5 secondes.
                    KeyValueIterator<Windowed<String>, Long> fetchAll = windowStore.fetchAll(from, now);
                    //WindowStoreIterator<Long> fetchAll = windowStore.fetch(page, from, now);
                    while (fetchAll.hasNext()){
                        KeyValue<Windowed<String>, Long> next = fetchAll.next();
                        stringLongMap.put(next.key.key(),next.value);
                    }
                    return stringLongMap;
                });
    }
}
