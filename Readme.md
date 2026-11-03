# TP : Architecture Événementielle avec Kafka & Spring Cloud Stream


Mise en place d'une architecture event-driven avec Kafka, Spring Cloud Stream, et Kafka Streams pour du traitement de données en temps réel.

![img.png](img.png)
##  Étapes

### 1. Démarrage Kafka

Après ajouter un  fichier compose.yml pour kafka et zookepper , on a demaré le service kafka avec commande docker compose - up
![img_1.png](img_1.png)

Une fois les conteneurs démarrés (`zookeeper` et `kafka-broker`), on vérifie qu'ils tournent bien avec `docker ps` :
![img_2.png](img_2.png)

---

### 2. Test avec Kafka Console

On teste d'abord le broker avec un **producer** et un **consumer** en ligne de commande via `docker exec` :

- On lance un **consumer** sur le topic `T1` :
  ![img_4.png](img_4.png)
![img_5.png](img_5.png)

- Puis on lance un **producer** sur le même topic et on envoie un message :
  ![img_19.png](img_19.png)

> Le consumer reçoit bien le message envoyé par le producer.

On remarque un petit **délai** dans la réception des messages : c'est dû au fait que le Kafka Consumer utilise la technique du **polling** (par défaut toutes les 1 seconde).


---

### 3. Création d'un REST Controller (Producer Spring)

On crée un `RestController` qui publie un `PageEvent` (nom de page, utilisateur, date, durée) dans un topic Kafka via **StreamBridge** de Spring Cloud Stream :

- On teste avec un consumer sur console kafka en premier :
![img_7.png](img_7.png)


- Puis on teste avec un appel HTTP `/publish?name=P1&topic=T1` et on vérifie que le message arrive bien dans le topic :
![img_6.png](img_6.png)

---

### 4. Création d'un Consumer Spring

On crée un `Consumer<PageEvent>` annoté avec `@Bean` pour consommer les messages d'un topic. Il faut déclarer la destination et la fonction dans `application.properties` :
![img_11.png](img_11.png)

On redémarre et on vérifie dans la console que les messages sont bien consommés :
![img_8.png](img_8.png)

---

### 5. Création d'un Supplier (Producer automatique)

On crée un `Supplier<PageEvent>` qui génère automatiquement un événement à intervalle régulier (par défaut 1s, réglable via `spring.cloud.stream.poller.fixed-delay`) :
![img_10.png](img_10.png)

On déclare le binding `out-0` vers un topic (ex: `T3`), puis on observe le flux de messages arriver chaque seconde dans le topic :
![img_9.png](img_9.png)

---

### 6. Kafka Streams (Traitement temps réel)

On crée une `Function<KStream, KStream>` qui consomme le stream produit par le supplier (`T3`), applique un `filter`, un `map`, un `groupByKey`, un `windowedBy(5 secondes)` et un `count`, puis publie le résultat dans un autre topic (`T4`) :
![img_15.png](img_15.png)

Configuration des bindings `in-0` / `out-0` et du `commit.interval.ms` pour un rendu temps réel :
![img_14.png](img_14.png)

---

### 7. Visualisation Temps Réel

On expose un endpoint `/analytics` en **Server-Sent Events** (`TEXT_EVENT_STREAM`) qui lit le `count-store` (KTable matérialisée par Kafka Streams) et pousse les résultats au frontend chaque seconde :
![img_17.png](img_17.png)

Côté frontend, une simple page `index.html` utilise **Smoothie Charts** et un `EventSource` pour afficher en temps réel le nombre de visites des pages P1 et P2 sur les 5 dernières secondes :
![img_16.png](img_16.png)

Résultat final : un dashboard qui affiche les deux courbes (P1 en vert, P2 en rouge) mises à jour en temps réel :
![img_18.png](img_18.png)