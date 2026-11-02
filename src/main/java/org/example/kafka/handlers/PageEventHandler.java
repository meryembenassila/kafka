package org.example.kafka.handlers;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.example.kafka.events.PageEvent;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.cloud.stream.binder.kafka.streams.InteractiveQueryService;
import org.springframework.context.annotation.Bean;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.Date;
import java.util.Random;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;

@Component
public class PageEventHandler {

    @Autowired
    private InteractiveQueryService interactiveQueryService;


    @Bean
    public Consumer<PageEvent> pageEventConsumer() {//le consumer doit faire un subsribe vers un topic(voir application.propreties)
        return (input) -> {
            System.out.println("*************************");
            System.out.println(input.toString());
            System.out.println("*************************");

        };
    }
    @Bean
    public Supplier<PageEvent> pageEventSupplier(){//le supplier doit produire un messge  vers un topic(voir application.propreties)
        return ()-> {
            return new PageEvent(
                    Math.random()<0.5?"P1":"P2",
                    Math.random()<0.5?"U1":"U2",
                    new Date(),
                    10+new Random().nextInt(1000));

        };
    }
    @Bean
    public Function<KStream<String,PageEvent>,KStream<String,Long>>
    KSteamFunction(){//le kstrem  doit consomer et produire apres un traitement  un messge  depuis et vers un topic(voir application.propreties)
        return (input) ->
                input
                        // 1. Garder les événements dont la durée > 100
                        .filter((k, v) -> v.duration() > 100)
                        // 2. Utiliser le nom de la page comme clé
                        //    et la durée comme valeur
                        .map((k, v) ->
                                new KeyValue<>(v.name(), v.duration()))
                        // 3. Regrouper les événements par nom de page
                        .groupByKey(
                                Grouped.with(
                                        Serdes.String(),
                                        Serdes.Long()
                                )
                        )
                        .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofSeconds(5)))
                        // 4. Compter les événements pour chaque page
                        .count(Materialized.as("count-store"))
                        // 5. Convertir la KTable en KStream
                        .toStream()
                         //Après le fenêtrage, la clé n'est plus un simple String, mais un Windowed<String>.
                        .map((k,v)->new KeyValue<>(k.key(), v));
    }
}
