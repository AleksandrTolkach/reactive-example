package by.toukach.reactiveexample;

import by.toukach.reactiveexample.dto.Player;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.function.Tuple2;

public class PublisherTest {

  private final List<String> characterList = List.of("Garfield", "Kojak", "Barbossa");
  private final List<String> foodList = List.of("Lasagna", "Lollipops", "Apples");
  private final String[] fruitArray =
      new String[]{"Apple", "Orange", "Grape", "Banana", "Strawberry"};
  private final String EATS_STRING = "%s eats %s";
  private final List<String> playerList = List.of("Michael Jordan", "Scottie Pippen", "Steve Kerr");

  @Test
  void createFlux_just() {
    Flux<String> fruitFlux =
        Flux.just(fruitArray[0], fruitArray[1], fruitArray[2], fruitArray[3], fruitArray[4]);

    fruitFlux.subscribe(f -> String.format("Here is some fruit %s", f));

    testFluxFruits(fruitFlux);
  }

  @Test
  void createAFlux_fromArray() {
    Flux<String> fruitFlux = Flux.fromArray(fruitArray);

    testFluxFruits(fruitFlux);
  }

  @Test
  void createAFlux_fromIterable() {
    List<String> fruitList = Arrays.asList(fruitArray);

    Flux<String> fruitFlux = Flux.fromIterable(fruitList);

    testFluxFruits(fruitFlux);
  }

  @Test
  void createAFlux_range() {
    Flux<Integer> intervalFlux = Flux.range(1, 5);

    StepVerifier.create(intervalFlux)
        .expectNext(1)
        .expectNext(2)
        .expectNext(3)
        .expectNext(4)
        .expectNext(5)
        .verifyComplete();
  }

  @Test
  void createAFlux_interval() {
    Flux<Long> intervalFlux = Flux.interval(Duration.ofSeconds(1))
        .take(5);

    StepVerifier.create(intervalFlux)
        .expectNext(0L)
        .expectNext(1L)
        .expectNext(2L)
        .expectNext(3L)
        .expectNext(4L)
        .verifyComplete();
  }

  @Test
  void mergeFluxes() {
    Flux<String> characterFlux = Flux.fromIterable(characterList)
        .delayElements(Duration.ofMillis(500));
    Flux<String> foodFlux = Flux.fromIterable(foodList)
        .delaySubscription(Duration.ofMillis(250))
        .delayElements(Duration.ofMillis(500));

    Flux<String> mergedFlux = characterFlux.mergeWith(foodFlux);

    StepVerifier.create(mergedFlux)
        .expectNext(characterList.get(0))
        .expectNext(foodList.get(0))
        .expectNext(characterList.get(1))
        .expectNext(foodList.get(1))
        .expectNext(characterList.get(2))
        .expectNext(foodList.get(2))
        .verifyComplete();
  }

  @Test
  void zipFluxes() {
    Flux<String> characterFlux = Flux.fromIterable(characterList);
    Flux<String> foodFlux = Flux.fromIterable(foodList);

    Flux<Tuple2<String, String>> zippedFlux = Flux.zip(characterFlux, foodFlux);

    StepVerifier.create(zippedFlux)
        .expectNextMatches(p ->
            Optional.of(p.getT1())
                .filter(t -> t.equals(characterList.get(0)))
                .isPresent() &&
                Optional.of(p.getT2())
                    .filter(t -> t.equals(foodList.get(0)))
                    .isPresent())
        .expectNextMatches(p ->
            Optional.of(p.getT1())
                .filter(t -> t.equals(characterList.get(1)))
                .isPresent() &&
                Optional.of(p.getT2())
                    .filter(t -> t.equals(foodList.get(1)))
                    .isPresent())
        .expectNextMatches(p ->
            Optional.of(p.getT1())
                .filter(t -> t.equals(characterList.get(2)))
                .isPresent() &&
                Optional.of(p.getT2())
                    .filter(t -> t.equals(foodList.get(2)))
                    .isPresent())
        .verifyComplete();
  }

  @Test
  void zippedFluxToObject() {
    Flux<String> characterFlux = Flux.fromIterable(characterList);
    Flux<String> foodFlux = Flux.fromIterable(foodList);

    Flux<String> zippedFlux = Flux.zip(characterFlux, foodFlux, (c, f) ->
        String.format(EATS_STRING, c, f));

    StepVerifier.create(zippedFlux)
        .expectNext(String.format(EATS_STRING, characterList.get(0), foodList.get(0)))
        .expectNext(String.format(EATS_STRING, characterList.get(1), foodList.get(1)))
        .expectNext(String.format(EATS_STRING, characterList.get(2), foodList.get(2)))
        .verifyComplete();
  }

  @Test
  void firstWithSignalFlux() {
    Flux<String> characterFlux = Flux.fromIterable(characterList);
    Flux<String> foodFlux = Flux.fromIterable(foodList)
        .delayElements(Duration.ofMillis(100));

    Flux<String> firstFlux = Flux.firstWithSignal(characterFlux, foodFlux);

    StepVerifier.create(firstFlux)
        .expectNext(characterList.get(0))
        .expectNext(characterList.get(1))
        .expectNext(characterList.get(2))
        .verifyComplete();
  }

  @Test
  void skipAFew() {
    Flux<String> fruitFlux = Flux.fromArray(fruitArray)
        .skip(3);

    StepVerifier.create(fruitFlux)
        .expectNext(fruitArray[3])
        .expectNext(fruitArray[4])
        .verifyComplete();
  }

  @Test
  void skipAFewSeconds() {
    Flux<String> fruitFlux = Flux.fromArray(fruitArray)
        .delayElements(Duration.ofSeconds(1))
        .skip(Duration.ofSeconds(4));

    StepVerifier.create(fruitFlux)
        .expectNext(fruitArray[3])
        .expectNext(fruitArray[4])
        .verifyComplete();
  }

  @Test
  void take() {
    Flux<String> fruitFlux = Flux.fromArray(fruitArray)
        .take(3);

    StepVerifier.create(fruitFlux)
        .expectNext(fruitArray[0])
        .expectNext(fruitArray[1])
        .expectNext(fruitArray[2])
        .verifyComplete();
  }

  @Test
  void takeForAWhile() {
    Flux<String> fruitFlux = Flux.fromArray(fruitArray)
        .delayElements(Duration.ofSeconds(1))
        .take(Duration.ofMillis(3500));

    StepVerifier.create(fruitFlux)
        .expectNext(fruitArray[0])
        .expectNext(fruitArray[1])
        .expectNext(fruitArray[2])
        .verifyComplete();
  }

  @Test
  void filter() {
    Flux<String> filteredFlux = Flux.fromArray(fruitArray)
        .filter(fruit -> fruit.contains("n"));

    StepVerifier.create(filteredFlux)
        .expectNext(fruitArray[1])
        .expectNext(fruitArray[3])
        .verifyComplete();
  }

  @Test
  void distinct() {
    List<String[]> duplicateFruitList = List.of(fruitArray, fruitArray);

    Flux<String[]> distinctFlux = Flux.fromIterable(duplicateFruitList)
        .distinct();

    StepVerifier.create(distinctFlux)
        .expectNext(fruitArray)
        .verifyComplete();
  }

  @Test
  void map() {
    Flux<Player> mappedFlux = Flux.fromIterable(playerList)
        .map(player -> {
          String[] nameArray = player.split(" ");
          return new Player(nameArray[0], nameArray[1]);
        });

    StepVerifier.create(mappedFlux)
        .expectNext(new Player("Michael", "Jordan"))
        .expectNext(new Player("Scottie", "Pippen"))
        .expectNext(new Player("Steve", "Kerr"))
        .verifyComplete();
  }

  @Test
  void flatMap() {
    Flux<Player> playerFlux = Flux.fromIterable(playerList)
        .flatMap(player -> Mono.just(player)
            .map(p -> {
              String[] nameArray = player.split(" ");
              return new Player(nameArray[0], nameArray[1]);
            }))
        .subscribeOn(Schedulers.parallel());

    List<Player> expectedPlayers = List.of(
        new Player("Michael", "Jordan"),
        new Player("Scottie", "Pippen"),
        new Player("Steve", "Kerr"));

    StepVerifier.create(playerFlux)
        .expectNextMatches(expectedPlayers::contains)
        .expectNextMatches(expectedPlayers::contains)
        .expectNextMatches(expectedPlayers::contains)
        .verifyComplete();
  }

  @Test
  void buffer() {
    Flux<List<String>> bufferedFlux = Flux.fromArray(fruitArray)
        .buffer(3);

    StepVerifier.create(bufferedFlux)
        .expectNext(List.of(fruitArray[0], fruitArray[1], fruitArray[2]))
        .expectNext(List.of(fruitArray[3], fruitArray[4]))
        .verifyComplete();
  }

  @Test
  void bufferedAndAFlatMap() {
    Flux<String> twoListFlux = Flux.fromArray(fruitArray)
        .buffer(3)
        .flatMap(f -> Flux.fromIterable(f)
            .map(String::toUpperCase)
            .subscribeOn(Schedulers.parallel())
            .log());

    twoListFlux.subscribe();

    StepVerifier.create(twoListFlux)
        .expectNextMatches(l -> l.contains(fruitArray[0].toUpperCase()))
        .expectNextMatches(l -> l.contains(fruitArray[1].toUpperCase()))
        .expectNextMatches(l -> l.contains(fruitArray[2].toUpperCase()))
        .expectNextMatches(l -> l.contains(fruitArray[3].toUpperCase()))
        .expectNextMatches(l -> l.contains(fruitArray[4].toUpperCase()))
        .verifyComplete();
  }

  @Test
  void collectList() {
    Mono<List<String>> fruitListMono = Flux.fromArray(fruitArray)
        .collectList();

    StepVerifier.create(fruitListMono)
        .expectNext(List.of(fruitArray))
        .verifyComplete();
  }

  @Test
  void collectToMap() {
    Arrays.stream(fruitArray)
            .collect(Collectors.toMap(f -> f.charAt(0), f -> f));

    Mono<Map<Character, String>> fruitMap = Flux.fromArray(fruitArray)
        .collectMap(f -> f.toLowerCase().charAt(0));

    StepVerifier.create(fruitMap)
        .expectNextMatches(map -> {
          return map.size() == 5 &&
              map.get('a').equals("Apple") &&
              map.get('o').equals("Orange") &&
              map.get('g').equals("Grape") &&
              map.get('b').equals("Banana") &&
              map.get('s').equals("Strawberry");
        })
        .verifyComplete();
  }

  @Test
  void allAny() {
    Mono<Boolean> isAllAMono = Flux.fromArray(fruitArray)
        .all(f -> f.toLowerCase().contains("a"));

    StepVerifier.create(isAllAMono)
        .expectNext(true)
        .verifyComplete();

    Mono<Boolean> isAllYMono = Flux.fromArray(fruitArray)
        .all(f -> f.toLowerCase().contains("y"));

    StepVerifier.create(isAllYMono)
        .expectNext(false)
        .verifyComplete();

    Mono<Boolean> isAnyYMono = Flux.fromArray(fruitArray)
        .any(f -> f.toLowerCase().contains("y"));

    StepVerifier.create(isAnyYMono)
        .expectNext(true)
        .verifyComplete();

    Mono<Boolean> isAnyZMono = Flux.fromArray(fruitArray)
        .any(f -> f.toLowerCase().contains("z"));

    StepVerifier.create(isAnyZMono)
        .expectNext(false)
        .verifyComplete();
  }

  private void testFluxFruits(Flux<String> fruitFlux) {
    StepVerifier.create(fruitFlux)
        .expectNext(fruitArray[0])
        .expectNext(fruitArray[1])
        .expectNext(fruitArray[2])
        .expectNext(fruitArray[3])
        .expectNext(fruitArray[4])
        .verifyComplete();
  }
}
