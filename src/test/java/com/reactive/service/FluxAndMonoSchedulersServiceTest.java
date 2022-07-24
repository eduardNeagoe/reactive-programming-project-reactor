package com.reactive.service;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.ParallelFlux;
import reactor.test.StepVerifier;

class FluxAndMonoSchedulersServiceTest {

    FluxAndMonoSchedulersService fluxAndMonoThread = new FluxAndMonoSchedulersService();

    @Test
    void publishOn() {
        //when
        var flux = fluxAndMonoThread.publishOn();

        //then
        StepVerifier.create(flux)
                .expectNextCount(6)
                .verifyComplete();

    }

    @Test
    void parallel() {
        //when
        ParallelFlux<String> flux = fluxAndMonoThread.parallel();

        //then
        StepVerifier.create(flux)
                .expectNextCount(3)
                // we cannot assert "ALEX", "BEN","CHLOE"
                // the order of the 3 names is not guaranteed
                .verifyComplete();
    }

    @Test
    void parallel_usingFlatMap() {
        //when
        Flux<String> flux = fluxAndMonoThread.parallel_usingFlatMap();

        //then
        StepVerifier.create(flux)
                .expectNextCount(3)
                // we cannot assert "ALEX", "BEN","CHLOE"
                // the order of the 3 names is not guaranteed
                .verifyComplete();
    }

    @Test
    void parallel_usingFlatMap_1() {
        //when
        Flux<String> flux = fluxAndMonoThread.parallel_usingFlatMap_1();

        //then
        StepVerifier.create(flux)
                .expectNextCount(6)
                .verifyComplete();
    }

    @Test
    void parallel_usingFlatMapSequential() {
        //when
        Flux<String> flux = fluxAndMonoThread.parallel_usingFlatMapSequential();

        //then
        StepVerifier.create(flux)
                .expectNext("ALEX", "BEN", "CHLOE")
                .verifyComplete();
    }

    @Test
    void parallel_1() {
        //when
        var flux = fluxAndMonoThread.parallel_1();

        //then
        StepVerifier.create(flux)
                .expectNextCount(6)
                .verifyComplete();

    }

    @Test
    void subscribeOn() {
        //when
        var flux = fluxAndMonoThread.
                subscribeOn();

        //then
        StepVerifier.create(flux)
                .expectNextCount(6)
                .verifyComplete();

    }

    @Test
    void subscribeOn_publishOn() {
        //when
        var flux = fluxAndMonoThread.
                subscribeOn_publishOn();

        //then
        StepVerifier.create(flux)
                .expectNextCount(6)
                .verifyComplete();

    }
}