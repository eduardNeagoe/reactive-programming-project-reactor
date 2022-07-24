package com.reactive.service;

import lombok.extern.slf4j.Slf4j;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.publisher.ParallelFlux;
import reactor.core.scheduler.Schedulers;

import java.time.Duration;
import java.util.List;

import static com.reactive.util.Util.delay;

@Slf4j
public class FluxAndMonoSchedulersService {

    static List<String> namesList = List.of("alex", "ben", "chloe");
    static List<String> namesList1 = List.of("adam", "jill", "jack");

    public Flux<String> publishOn() {
        // start without publish on
        // add publishon Schedulers.parallel()
        // add publishon Schedulers.boundedElastic() for the second flux


        var namesFlux = Flux.fromIterable(namesList)
                .publishOn(Schedulers.parallel())
                .map(this::upperCase)
                .log();

        var namesFlux1 = Flux.fromIterable(namesList1)
                .publishOn(Schedulers.boundedElastic())
                .map(this::upperCase)
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .log();

        return namesFlux.mergeWith(namesFlux1);
    }

    public ParallelFlux<String> parallel() {
        log.info("Number of cores : {}", Runtime.getRuntime().availableProcessors());

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .parallel()
                .runOn(Schedulers.parallel())
                .map(this::upperCase)
                .log();
    }

    public Flux<String> parallel_usingFlatMap() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .flatMap(name ->
                        Mono.just(name)
                                .map(this::upperCase)
                                .subscribeOn(Schedulers.parallel()))
                .log();
    }

    public Flux<String> parallel_usingFlatMap_1() {
        // start without publish on
        // add publishOn Schedulers.parallel()
        // add publishOn Schedulers.boundedElastic() for the second flux

        var namesFlux = Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .flatMap(name ->
                        Mono.just(name)
                                .map(this::upperCase)
                                .subscribeOn(Schedulers.parallel()))
                .log();

        var namesFlux1 = Flux.fromIterable(List.of("adam", "jill", "jack"))
                .flatMap(name ->
                        Mono.just(name)
                                .map(this::upperCase)
                                .subscribeOn(Schedulers.parallel()))
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .log();

        return namesFlux.mergeWith(namesFlux1);
    }

    public Flux<String> parallel_usingFlatMapSequential() {

        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .flatMapSequential(
                        name -> Mono.just(name)
                                .map(this::upperCase)
                                .subscribeOn(Schedulers.parallel()))
                .log();
    }


    public ParallelFlux<String> parallel_1() {
        // start without publishOn
        // add publishOn Schedulers.parallel()
        // add publishOn Schedulers.boundedElastic() for the second flux


        var namesFlux = Flux.fromIterable(namesList)
                .publishOn(Schedulers.parallel())
                .map(this::upperCase)
                .log();

        var namesFlux1 = Flux.fromIterable(namesList1)
                .publishOn(Schedulers.boundedElastic())
                .map(this::upperCase)
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .log();

        return namesFlux.mergeWith(namesFlux1)
                .parallel()
                .runOn(Schedulers.parallel());
    }


    public Flux<String> subscribeOn() {
        var namesFlux = flux1()
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .log();

        var namesFlux1 = flux2()
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map((s) -> {
                    log.info("Value of s after boundedElastic is {}", s);
                    return s;
                })
                .log();

        return namesFlux.mergeWith(namesFlux1);
    }

    public Flux<String> subscribeOn_publishOn() {
        var namesFlux = flux1()
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.parallel())
                .map((s) -> {
                    log.info("Value of s after publishOn is {}", s);
                    return s;
                })
                .log();

        var namesFlux1 = flux2()
                .map((s) -> {
                    log.info("Value of s is {}", s);
                    return s;
                })
                .subscribeOn(Schedulers.boundedElastic())
                .publishOn(Schedulers.parallel())
                .map((s) -> {
                    log.info("Value of s after publishOn is {}", s);
                    return s;
                })
                .log();

        return namesFlux.mergeWith(namesFlux1);
    }

    public Flux<String> flux1() {
        return Flux.fromIterable(namesList)
                .map(this::upperCase);
    }

    public Flux<String> flux2() {
        return Flux.fromIterable(namesList1)
                .map(this::upperCase);
    }

    private String upperCase(String name) {
        delay(1000);
        return name.toUpperCase();
    }

    public static void main(String[] args) throws InterruptedException {

        Flux.just("hello")
                .doOnNext(v -> System.out.println("just " + Thread.currentThread().getName()))
                .publishOn(Schedulers.boundedElastic())
                .doOnNext(v -> System.out.println("publish 0" + Thread.currentThread().getName()))
                .delayElements(Duration.ofMillis(500))
                .doOnNext(v -> System.out.println("publish 1" + Thread.currentThread().getName()))
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(v -> System.out.println(v + " delayed " + Thread.currentThread().getName()));

        Thread.sleep(5000);
    }
}
