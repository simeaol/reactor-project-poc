package com.slamine.reactive.test;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.blockhound.BlockHound;
import reactor.blockhound.BlockingOperationError;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

/**
 * Reactive Streams Core concepts:
 * 1- Asynchronous
 * 2- Non-blocking
 * 3- Backpressure
 *
 * Impl:
 * Publisher <- (subscribe) Subscriber
 * Subscription is created
 * Publisher (onSubscribe with the subscription) -> Subscriber
 * Subscription <- (request N) Subscriber
 * Publisher -> (onNext) Subscriber
 * Until:
 * 1. Publisher sends all the objects requested.
 * 2. Publisher sends all the objects it has. (onComplete) subscriber and subscription will be canceled.
 * 3. There is an error. (onError) -> subscriber and subscription will be canceled.
 */
@Slf4j
public class MonoTest {

    @BeforeAll
    public static void setUp(){
        BlockHound.install();
    }

    @Test
    public void blockHound(){
        try{
            FutureTask<?> task = new FutureTask<>(() -> {
                Thread.sleep(0);
                return "";
            });
            Schedulers.parallel().schedule(task);
            task.get(10, TimeUnit.SECONDS);
            Assertions.fail("should fail");
        }catch (Exception e){
            Assertions.assertTrue(e.getCause() instanceof BlockingOperationError);
        }
    }

    @Test
    public void monoSubscriber(){
        String name = "slamine";
        Mono<String> mono = Mono.just(name)
                .log();
        mono.subscribe();

        log.info("--------------------------------");

        StepVerifier.create(mono)
            .expectNext(name)
            .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumer(){
        String name = "slamine";
        Mono<String> mono = Mono.just(name)
                .log();
        mono.subscribe(s -> log.info("Value of {}", s));

        log.info("--------------------------------");

        StepVerifier.create(mono)
                .expectNext(name)
                .verifyComplete();
    }

    @Test
    public void monoSubscriberConsumerError(){
        String name = "slamine";
        Mono<String> mono = Mono.just(name)
                .map(s -> {
                    throw new RuntimeException("Test nano with error");
                });

        mono.subscribe(s -> log.info("Name {}", s), Throwable::printStackTrace);

        log.info("--------------------------------");

        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    public void monoSubscriberConsumerComplete(){
        String name = "slamine";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);
        mono.subscribe(s -> log.info("Value of {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED"));

        log.info("--------------------------------");

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoSubscriberSubscription(){
        String name = "slamine";
        Mono<String> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase);
        mono.subscribe(s -> log.info("Value of {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED"),
                //Subscription::cancel
                subscription -> subscription.request(5)
        );

        log.info("--------------------------------");

        StepVerifier.create(mono)
                .expectNext(name.toUpperCase())
                .verifyComplete();
    }

    @Test
    public void monoDoOnMethods(){
        String name = "slamine";
        Mono<Object> mono = Mono.just(name)
                .log()
                .map(String::toUpperCase)
                .doOnSubscribe(subscription -> log.info("subscribed {}", subscription))
                .doOnRequest(logNumber -> log.info("Request Received, starting doing something...."))
                .doOnNext(s -> log.info("Value is here. Executing doOnNext {}", s))
                .flatMap(s -> Mono.empty())
                .doOnNext(o -> log.info("Value is here. Executing doOnNext {}", o)) //will be ignored, cause the value is empty
                .doOnSuccess(s -> log.info("doOnSuccess executed. value={}", s)) //the value here will be null
                ;
        mono.subscribe(s -> log.info("Value of {}", s),
                Throwable::printStackTrace,
                () -> log.info("FINISHED"),
                //Subscription::cancel
                subscription -> subscription.request(5)
        );

        log.info("--------------------------------");
    }

    @Test
    public void monoDoOnError(){
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .doOnNext(o -> log.info("Executing this doOnNext"))//will not be executed, cause doOnError will close the subscription
                .log();

        StepVerifier.create(error)
                .expectError(IllegalArgumentException.class)
                .verify();


    }

    @Test
    public void monoDoOnErrorResume(){
        String name = "simeao";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
                .doOnError(e -> log.error("Error message: {}", e.getMessage()))
                .onErrorResume(o -> {
                    log.info("Executing on error resume: {}", o.getMessage());
                    return Mono.just(name);
                })//will not be executed, cause doOnError will close the subscription
                .log();

        log.info("---------------monoDoOnErrorResume-----------------");

        StepVerifier.create(error)
                .expectNext(name)
                .verifyComplete();


    }

    @Test
    public void monoDoOnErrorReturn(){
        String name = "simeao";
        Mono<Object> error = Mono.error(new IllegalArgumentException("Illegal argument exception"))
                .onErrorReturn("EMPTY")
                /*.onErrorResume(o -> { //will not be executed, cause doOnError will close the subscription
                    log.info("Executing on error resume: {}", o.getMessage());
                    return Mono.just(name);
                })
                .doOnError(e -> MonoTest.log.error("Error message", e.getMessage()))
                */
                .log();

        log.info("---------------monoDoOnErrorResume-----------------");

        StepVerifier.create(error)
                .expectNext("EMPTY")
                .verifyComplete();


    }
}
