package com.slamine.reactive.test;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

@Slf4j
public class OperatorsTest {

    @BeforeAll
    public static void setUp(){
        BlockHound.install(builder ->
            builder.allowBlockingCallsInside("", "")
        );
    }

    @Test
    public void subscribeOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic()) //Affect both previous of after instructions declared
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    @Test
    public void publishOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.single()) //Affect only the instruction declared after publishOn
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        //How much subscriber more threads are started to deal with the request
//        flux.subscribe();
//        flux.subscribe();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    /**
     * This approach should be avoided.
     * In this case the first subscribedOn will be used for all published events which means, the second will be ignored
     */
    @Test
    public void multipleSubscribeOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single()) //
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    /**
     * This approach should be avoided.
     * In this case the first subscribedOn will execute only the map 1
     * and the second one will be executed by boundedElastic
     */
    @Test
    public void multiplePublishOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) //
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    /**
     * This approach should be avoided.
     * In this case the first subscribedOn will executed and the subscriptionOn will be ignored
     */
    @Test
    public void publishAndSubscribeOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .publishOn(Schedulers.single()) //
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    /**
     * This approach should be avoided.
     *
     */
    @Test
    public void subscribeAndPublishOnSimple(){
        Flux<Integer> flux = Flux.range(1, 4)
                .subscribeOn(Schedulers.single()) //
                .map(i -> {
                    log.info("Map-1 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                })
                .publishOn(Schedulers.boundedElastic())
                .map(i -> {
                    log.info("Map-2 - number {} on Thread {}", i, Thread.currentThread().getName());
                    return i;
                });

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext(1,2,3,4)
                .verifyComplete();
    }

    /**
     * From callable should be used whenever you made external calls (for api or others)
     *
     */
    @Test
    public void subscribeOnIO(){
        Mono<List<String>> list = Mono.fromCallable(() -> Files.readAllLines(Path.of("text-file")))
                .log()
                .subscribeOn(Schedulers.boundedElastic());

        list.subscribe(s -> log.info("{}", s));

        //Thread.sleep(2000);

        StepVerifier.create(list)
                .expectSubscription()
                .thenConsumeWhile(l -> {
                    Assertions.assertFalse(l.isEmpty());
                    log.info("size: {}", l.size());
                    return true;
                })
                .verifyComplete();

    }

    /**
     * WhichIfEmpty is the operator you should use when you subscribe to an publisher
     * without any message available
     */
    @Test
    public void switchIfEmptyOperator(){
        Flux<Object> flux = emptyFlux()
                .switchIfEmpty(Flux.just("Not empty anymore"))
                .log();

        StepVerifier.create(flux)
                .expectSubscription()
                .expectNext("Not empty anymore")
                .expectComplete()
                .verify();
    }

    /**
     * Defer operator delay execution of events inside the operator
     */
    @Test
    public void deferOperator() {
        //Mono<Long> just = Mono.just(System.currentTimeMillis()); // emits the specified item, which is captured at instantiation time.


        Mono<Long> defer = Mono.defer(() -> Mono.just(System.currentTimeMillis()));//Executed whenever on subscribe event

        defer.subscribe(l -> log.info("time {}", l));
        defer.subscribe(l -> log.info("time {}", l));
        defer.subscribe(l -> log.info("time {}", l));
        defer.subscribe(l -> log.info("time {}", l));

        AtomicLong atomicLong = new AtomicLong();
        defer.subscribe(atomicLong::set);
        Assertions.assertTrue(atomicLong.get() > 0);


    }

    private Flux<Object> emptyFlux(){
        return Flux.empty();
    }


    /**
     * Concat merge both flux. Subscribe to the first flux and then to the second one
     */
    @Test
    public void concatOperator(){
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatFlux = Flux.concat(flux1, flux2)
                .log();

        StepVerifier.create(concatFlux)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    @Test
    //TODO: Something weird. This test should expect error but it is been completed, why ?
    public void concatOperatorError() {
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if(s.equals("b")){
                        throw new IllegalArgumentException("Illegal arguments s==b");
                    }
                    return s;
                });
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concat = flux1.concat(flux2) //checkout concatDelayError
                .log();


        StepVerifier.create(concat)
                .expectSubscription()
                .expectNext("c", "d")
               // .expectError() Why not error ?
                .expectComplete()
                .verify();
    }

    /**
     * Same thing with @concatOperator
     */
    @Test
    public void concatWithOperator(){
        Flux<String> flux1 = Flux.just("a", "b");
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> concatWith = flux1.concatWith(flux2)
                .log();


        StepVerifier.create(concatWith)
                .expectSubscription()
                .expectNext("a", "b", "c", "d")
                .expectComplete()
                .verify();
    }

    /**
     * Combine the last data emit by flux1 with the last of flux2
     */
    @Test
    public void combineLatestOperator(){
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofSeconds(1));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> combineLatest =
                Flux.combineLatest(flux1, flux2, (s1, s2) -> s1.toUpperCase() + s2.toUpperCase())
                .log();


        StepVerifier.create(combineLatest)
                .expectSubscription()
                .expectNext("AD","BD")
                .expectComplete()
                .verify();
    }

    /**
     * Run in paralel Threads eagerly
     * @throws Exception
     */
    @Test
    public void mergeOperator() throws Exception{
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.merge(flux1, flux2)
                .delayElements(Duration.ofMillis(200))
                .log();

        merge.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(merge)
        .expectSubscription()
        .expectNext("c", "d", "a", "b")
        .expectComplete()
        .verify();
    }

    @Test
    public void mergeDelayErrorOperator() throws Exception{
        Flux<String> flux1 = Flux.just("a", "b")
                .map(s -> {
                    if(s.equals("b")){
                        throw new IllegalArgumentException("Illegal arg exception s==b");
                    }
                    return s;
                }).doOnError(t -> log.error("We could be do something with this"));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.mergeDelayError(1, flux1, flux2, flux1)
                .log();

        merge.subscribe(log::info);

        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("a", "c", "d", "a")
                .expectError()
                .verify();
    }

    @Test
    public void mergeWithOperator() throws Exception{
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = flux1.mergeWith(flux2)
                .delayElements(Duration.ofMillis(200))
                .log();

        merge.subscribe(log::info);

        Thread.sleep(1000);

        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("c", "d", "a", "b")
                .expectComplete()
                .verify();


    }

    @Test
    public void mergeSequentialOperator() throws Exception{
        Flux<String> flux1 = Flux.just("a", "b").delayElements(Duration.ofMillis(200));
        Flux<String> flux2 = Flux.just("c", "d");

        Flux<String> merge = Flux.mergeSequential(flux1, flux2, flux1)
                .delayElements(Duration.ofMillis(200))
                .log();

        StepVerifier.create(merge)
                .expectSubscription()
                .expectNext("a", "b", "c", "d", "a", "b")
                .expectComplete()
                .verify();
    }
    
    @Test
    public void flatMapOperator() throws Exception{
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMap(this::findByName) //on delay of Flux(nameA1, nameA2) the order will not be respected so the test will fail
                .log();

        flatFlux.subscribe(s -> log.info(s));

        StepVerifier.create(flatFlux)
                .expectSubscription()
                //.expectNext("nomeA1", "nomeA2", "nomeB1", "nomeB2")
                .expectNext("nomeB1", "nomeB2", "nomeA1", "nomeA2")
                .expectComplete()
                .verify();
    }

    @Test
    public void flatMapSequentialOperator() throws Exception{
        Flux<String> flux = Flux.just("a", "b");
        Flux<String> flatFlux = flux.map(String::toUpperCase)
                .flatMapSequential(this::findByName)//grant the order of the elements regardless the delays
                .log();

        flatFlux.subscribe(log::info);

        StepVerifier.create(flatFlux)
                .expectSubscription()
                .expectNext("nomeA1", "nomeA2", "nomeB1", "nomeB2")
                .expectComplete()
                .verify();
    }

    public Flux<String> findByName(String name){
        return name.equals("A") ? Flux.just("nomeA1", "nomeA2").delayElements(Duration.ofMillis(100)) : Flux.just("nomeB1", "nomeB2");
    }

    @Test
    public void zipOperator(){
        Flux<String> titlesFlux = Flux.just("Grand Blue", "Baki");
        Flux<String> studiosFlux = Flux.just("Zero-G", "TMS Entertainment");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = Flux.zip(titlesFlux, studiosFlux, episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), tuple.getT2(), tuple.getT3())));

        animeFlux.subscribe(a -> log.info(a.toString()));

        StepVerifier.create(animeFlux)
                .expectSubscription()
                .expectNext(new Anime("Grand Blue", "Zero-G", 12))
                .expectNext(new Anime("Baki", "TMS Entertainment", 24))
                .expectComplete()
                .verify();
    }

    /**
     * Support only 2 fluxs(publishers)
     */
    @Test
    public void zipWithOperator(){
        Flux<String> titlesFlux = Flux.just("Grand Blue", "Baki");
        Flux<Integer> episodesFlux = Flux.just(12, 24);

        Flux<Anime> animeFlux = titlesFlux.zipWith(episodesFlux)
                .flatMap(tuple -> Flux.just(new Anime(tuple.getT1(), null, tuple.getT2())));

        animeFlux.subscribe(a -> log.info(a.toString()));

        StepVerifier.create(animeFlux)
                .expectSubscription()
                .expectNext(new Anime("Grand Blue", null, 12))
                .expectNext(new Anime("Baki", null, 24))
                .expectComplete()
                .verify();
    }
    

    @AllArgsConstructor
    @Getter
    @ToString
    @EqualsAndHashCode
    static class Anime {
        String title;
        String studio;
        Integer episode;
    }
}
