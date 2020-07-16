package com.slamine.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import reactor.blockhound.BlockHound;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;

@Slf4j
public class FluxTest {
    @BeforeAll
    public static void setUp(){
        BlockHound.install();
    }

    @Test
    public void fluxSubscriber(){
        String [] array = {"simeao", "david", "lamine", "corp"};
        Flux<String> flux = Flux.just(array)
                .log();

        StepVerifier.create(flux)
                .expectNext(array)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbers(){
//        Integer [] array = {1, 2, 3, 4};
//        Flux<Integer> flux = Flux.just(array)
//                .log();

        Flux<Integer> flux = Flux.range(1, 5)
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("---------------------------");

        StepVerifier.create(flux)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberFromList(){

        Flux<Integer> flux = Flux.fromIterable(List.of(1,2,3,4,5))
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("---------------------------");

        StepVerifier.create(flux)
                .expectNext(1,2,3,4,5)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberNumbersError(){

        Flux<Integer> flux = Flux.range(1,5)
                .log()
                .map(i -> {
                    if(i == 4){
                        throw new IndexOutOfBoundsException("index error");
                    }
                    return i;
                });

        flux.subscribe(i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("DONE!")
                );

        log.info("---------------------------");

        StepVerifier.create(flux)
                .expectNext(1,2,3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberNumbersBackPressure(){

        Flux<Integer> flux = Flux.range(1,5)
                .log()
                .map(i -> {
                    if(i == 4){
                        throw new IndexOutOfBoundsException("index error");
                    }
                    return i;
                });

        flux.subscribe(i -> log.info("Number {}", i),
                Throwable::printStackTrace,
                () -> log.info("DONE!"),
                subscription -> subscription.request(3) //will not throw exception, cause subscribe backpressure its only 3. But the way the DONE will not be executed
        );

        log.info("---------------------------");

        StepVerifier.create(flux)
                .expectNext(1,2,3)
                .expectError(IndexOutOfBoundsException.class)
                .verify();
    }

    @Test
    public void fluxSubscriberBackpressureWithLimitRate(){
        Flux<Integer> flux = Flux.range(1, 10)
                .limitRate(3)
                .log();

        flux.subscribe(i -> log.info("Number {}", i));

        log.info("-----------------");

        StepVerifier.create(flux)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .verifyComplete();
    }

    @Test
    public void fluxSubscriberIntervalOne() throws Exception {
        Flux<Long> interval = Flux.interval(Duration.ofMillis(100))
                .take(10)
                .log();

        interval.subscribe(i -> log.info("Number {}", i));

        Thread.sleep(3000);
    }

    @Test
    public void fluxSubscriberIntervalTwo(){
        StepVerifier.withVirtualTime(this::createInterval)
                .expectSubscription()
                .thenAwait(Duration.ofDays(2))
                .expectNext(0L)
                .expectNext(1L)
                .thenCancel()
                .verify();
    }

    private Flux<Long> createInterval(){
        return Flux.interval(Duration.ofDays(1))
                .log();
    }


    /**
     * Start publishing events regardless if there are consumer or not
     * @throws Exception
     */
    @Test
    public void connectableFlux() throws Exception{
        ConnectableFlux<Integer> connectableFlux = Flux.range(1, 10)
               // .log()
                .delayElements(Duration.ofMillis(100))
                .publish();

        connectableFlux.connect();
        
        log.info("-----------------Thread sleeping for 300ms----------------------");
       // Thread.sleep(300);
        connectableFlux.subscribe(i -> log.info("Sub-1 number {}", i));

        log.info("-----------------Thread sleeping for 200ms----------------------");
        //Thread.sleep(200);
        connectableFlux.subscribe(i -> log.info("Sub-2 number {}", i));

        StepVerifier
                .create(connectableFlux)
                .then(connectableFlux::connect)
                //.thenConsumeWhile(i -> i <= 5) //Consume while elements less then 5
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .expectComplete()
                .verify();
    }

    /**
     * In this test case we validate auto connect functionality. Which start publishing events
     * only when there are the number of consumer defined in the autoConnect method
     * @throws Exception
     */
    @Test
    public void connectableFluxAutoConnect() throws Exception{
        Flux<Integer> flexAutoConnect = Flux.range(1, 10)
                .log()
                .delayElements(Duration.ofMillis(100))
                .publish()
                .autoConnect(2);

        StepVerifier
                .create(flexAutoConnect)
                .then(flexAutoConnect::subscribe)
                .expectNext(1,2,3,4,5,6,7,8,9,10)
                .expectComplete()
                .verify();
    }

    @Test
    public void fluxSubscribeNumbersWithBackPressureOf2(){
        Flux<Integer> flux = Flux.range(1, 10)
                .log();

        flux.subscribe(new Subscriber<Integer>() {
            private int count;
            private Subscription subscription;
            @Override
            public void onSubscribe(Subscription subscription) {
                this.subscription = subscription;
                subscription.request(5);
            }

            @Override
            public void onNext(Integer integer) {
                count++;
                if(count >= 2){
                    count = 0;
                    subscription.request(2);
                }
            }

            @Override
            public void onError(Throwable throwable) {

            }

            @Override
            public void onComplete() {

            }
        });


    }

}
