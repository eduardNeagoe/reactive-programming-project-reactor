package com.reactive.service;

import com.reactive.exception.ReactorException;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Hooks;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.test.scheduler.VirtualTimeScheduler;
import reactor.tools.agent.ReactorDebugAgent;

import java.time.Duration;
import java.util.List;

class FluxAndMonoGeneratorServiceTest {

    FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

    @Test
    void namesFlux() {
        //when
        var stringFlux = fluxAndMonoGeneratorService.namesFlux();

        //then
        StepVerifier.create(stringFlux)
                //.expectNext("alex", "ben", "chloe")
                //.expectNextCount(3)
                .expectNext("alex")
                .expectNextCount(2)
                .verifyComplete();

    }

    @Test
    void namesFlux_Immutability() {
        //when
        var stringFlux = fluxAndMonoGeneratorService.namesFlux_immutability()
                .log();

        //then
        StepVerifier.create(stringFlux)
                //.expectNext("ALEX", "BEN", "CHLOE")
                .expectNextCount(3)
                .verifyComplete();


    }

    @Test
    void namesMono() {

        //given
        //when
        var stringMono = fluxAndMonoGeneratorService.namesMono();

        //then
        StepVerifier.create(stringMono)
                .expectNext("alex")
                .verifyComplete();

    }

    @Test
    void namesMono_map_filter() {

        //given
        int stringLength = 3;

        //when
        var stringMono = fluxAndMonoGeneratorService.namesMono_map_filter(stringLength);

        //then
        StepVerifier.create(stringMono)
                .expectNext("ALEX")
                .verifyComplete();

    }

    @Test
    void namesMono_map_empty() {

        //given
        int stringLength = 4;

        //when
        var stringMono = fluxAndMonoGeneratorService.namesMono_map_filter(stringLength);

        //then
        StepVerifier.create(stringMono)
                .expectNext("default")
                .verifyComplete();

    }


    @Test
    void namesFlux_map() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_map(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                //.expectNext("ALEX", "BEN", "CHLOE")
                .expectNext("4-ALEX", "5-CHLOE")
                .verifyComplete();

    }

    @Test
    void namesFlux_flatmap() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_flatmap(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();

    }

    @Test
    void namesFlux_flatmap_async() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_flatmap_async(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                /*.expectNext("0-A", "1-L", "2-E", "3-X")
                .expectNextCount(5)*/
                .expectNextCount(9)
                .verifyComplete();

    }

    @Test
    void namesFlux_concatMap() {
        //given
        int stringLength = 3;

        //when
        Flux<String> namesFlux = fluxAndMonoGeneratorService.namesFlux_concatmap(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }

    @Test
    void namesFlux_concatmap_withVirtualTime() {
        //given
        VirtualTimeScheduler.getOrSet();
        int stringLength = 3;

        //when
        Flux<String> namesFlux = fluxAndMonoGeneratorService.namesFlux_concatmap(stringLength);

        //then
        StepVerifier.withVirtualTime(() -> namesFlux)
                .thenAwait(Duration.ofSeconds(10))
                .expectNext("A", "L", "E", "X", "C", "H", "L", "O", "E")
                .verifyComplete();
    }

    @Test
    void namesMono_flatmap() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesMono_flatmap(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext(List.of("A", "L", "E", "X"))
                .verifyComplete();

    }

    @Test
    void namesMono_flatmapMany() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesMono_flatmapMany(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("A", "L", "E", "X")
                .verifyComplete();

    }


    @Test
    void namesFlux_transform() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_transform(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("A", "L", "E", "X")
                .expectNextCount(5)
                .verifyComplete();

    }

    @Test
    void namesFlux_transform_1() {

        //given
        int stringLength = 6;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_transform(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("default")
                //.expectNextCount(5)
                .verifyComplete();

    }

    @Test
    void namesFlux_transform_switchIfEmpty() {

        //given
        int stringLength = 6;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_transform_switchIfEmpty(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                .expectNext("D", "E", "F", "A", "U", "L", "T")
                //.expectNextCount(5)
                .verifyComplete();

    }

    @Test
    void namesFlux_transform_concatWith() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_transform_concatWith(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                //.expectNext("ALEX", "BEN", "CHLOE")
                .expectNext("4-ALEX", "5-CHLOE", "4-ANNA")
                .verifyComplete();

    }

    @Test
    void name_defaultIfEmpty() {
        //when
        var value = fluxAndMonoGeneratorService.name_defaultIfEmpty();

        //then
        StepVerifier.create(value)
                .expectNext("Default")
                .verifyComplete();

    }

    @Test
    void name_switchIfEmpty() {
        //when
        var value = fluxAndMonoGeneratorService.name_switchIfEmpty();

        //then
        StepVerifier.create(value)
                .expectNext("Default")
                .verifyComplete();

    }

    @Test
    void concat() {
        //when
        var value = fluxAndMonoGeneratorService.concat();

        //then
        StepVerifier.create(value)
                .expectNext("A", "B", "C", "D", "E", "F")
                .verifyComplete();

    }


    @Test
    void concatWith() {
        //when
        var value = fluxAndMonoGeneratorService.concatWith();

        //then
        StepVerifier.create(value)
                .expectNext("A", "B", "C", "D", "E", "F")
                .verifyComplete();

    }

    @Test
    void concat_mono() {
        //when
        var value = fluxAndMonoGeneratorService.concatWith_mono();

        //then
        StepVerifier.create(value)
                .expectNext("A", "B")
                .verifyComplete();

    }

    @Test
    void merge() {
        //when
        Flux<String> value = fluxAndMonoGeneratorService.merge();

        //then
        StepVerifier.create(value)
                .expectNext("A", "D", "B", "E", "C", "F")
                .verifyComplete();
    }

    @Test
    void mergeWith() {
        //when
        var value = fluxAndMonoGeneratorService.mergeWith();

        //then
        StepVerifier.create(value)

                .expectNext("A", "D", "B", "E", "C", "F")
                .verifyComplete();

    }

    @Test
    void mergeWith_mono() {
        //when
        var value = fluxAndMonoGeneratorService.mergeWith_mono();

        //then
        StepVerifier.create(value)

                .expectNext("A", "B")
                .verifyComplete();

    }

    @Test
    void mergeSequential() {
        //when
        Flux<String> value = fluxAndMonoGeneratorService.mergeSequential();

        //then
        StepVerifier.create(value)
                .expectNext("A", "B", "C", "D", "E", "F")
                .verifyComplete();
    }

    @Test
    void zip() {
        //when
        var value = fluxAndMonoGeneratorService.zip().log();

        //then
        StepVerifier.create(value)
                .expectNext("AD", "BE", "CF")
                .verifyComplete();
    }

    @Test
    void zip_1() {
        //when
        var value = fluxAndMonoGeneratorService.zip_1().log();

        //then
        StepVerifier.create(value)
                .expectNext("AD14", "BE25", "CF36")
                .verifyComplete();
    }


    @Test
    void zip_2() {
        //when
        Flux<String> value = fluxAndMonoGeneratorService.zip_Mono().log();

        //then
        StepVerifier.create(value)
                .expectNext("AB")
                .verifyComplete();
    }

    @Test
    void zipWith() {
        //when
        Flux<String> value = fluxAndMonoGeneratorService.zipWith().log();

        //then
        StepVerifier.create(value)
                .expectNext("AD", "BE", "CF")
                .verifyComplete();
    }

    @Test
    void zipWith_mono() {
        //when
        Mono<String> value = fluxAndMonoGeneratorService.zipWith_mono().log();

        //then
        StepVerifier.create(value)
                .expectNext("AB")
                .verifyComplete();
    }

    @Test
    void zipWith_mono_delay() {
        //when
        var value = fluxAndMonoGeneratorService.zipWith_mono_delay().log();

        //then
        StepVerifier.create(value)
                //.expectNext("AB")
                .expectError()
                .verify();

    }

    @Test
    void exception_flux() {
        //when
        Flux<String> flux = fluxAndMonoGeneratorService.exception_flux();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C")
                .expectError(RuntimeException.class)
                // you cannot call verifyComplete in here
                // we are expecting an error - there is no completion event
                .verify();
    }

    @Test
    void exception_flux_1() {
        //when
        var flux = fluxAndMonoGeneratorService.exception_flux();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C")
                .expectError()
                .verify(); // you cannot do a verifyComplete in here

    }

    @Test
    void exception_flux_2() {
        //when
        var flux = fluxAndMonoGeneratorService.exception_flux();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C")
                .expectErrorMessage("Exception Occurred")
                .verify(); // you cannot do a verifyComplete in here

    }


    @Test
    void OnErrorReturn() {
        //when
        Flux<String> flux = fluxAndMonoGeneratorService.OnErrorReturn().log();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C", "D")
                .verifyComplete();
    }


    @Test
    void OnErrorResume() {

        //given
        IllegalStateException e = new IllegalStateException("Not a valid state");

        //when
        Flux<String> flux = fluxAndMonoGeneratorService.OnErrorResume(e).log();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C", "D", "E", "F")
                .verifyComplete();
    }

    @Test
    void OnErrorResume_1() {

        //given
        var e = new RuntimeException("Not a valid state");

        //when
        var flux = fluxAndMonoGeneratorService.OnErrorResume(e).log();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C")
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    void OnErrorMap() {
        //when
        var flux = fluxAndMonoGeneratorService.OnErrorMap().log();

        //then
        StepVerifier.create(flux)
                .expectNext("A")
                .expectError(ReactorException.class)
                .verify();
    }

    @Test
    void OnErrorMap_checkpoint() {

        /*Error has been observed at the following site(s):
	        |_ checkpoint ⇢ errorSpot*/
        //given

        RuntimeException e = new RuntimeException("Not a valid state");

        //when
        Flux<String> flux = fluxAndMonoGeneratorService.OnErrorMap_checkpoint(e).log();

        //then
        StepVerifier.create(flux)
                .expectNext("A")
                .expectError(ReactorException.class)
                .verify();
    }


    /**
     * Gives the visibility of which operator caused the problem
     * Gives the "Assembly trace" which is not available when using checkpoint
     * Gives you the line that caused the problem
     */
    @Test
    void OnErrorMap_onOperatorDebug() {

        //You will see the below in the code
/*
        Error has been observed at the following site(s):
	|_      Flux.error ⇢ at com.learnreactiveprogramming.service.FluxAndMonoGeneratorService.OnErrorMap_checkpoint(FluxAndMonoGeneratorService.java:336)
	|_ Flux.concatWith ⇢ at com.learnreactiveprogramming.service.FluxAndMonoGeneratorService.OnErrorMap_checkpoint(FluxAndMonoGeneratorService.java:336)
	|_      checkpoint ⇢ errorSpot
*/

        //given
        Hooks.onOperatorDebug();
        RuntimeException e = new RuntimeException("Not a valid state");

        //when
        Flux<String> flux = fluxAndMonoGeneratorService.OnErrorMap_checkpoint(e).log();

        //then
        StepVerifier.create(flux)
                .expectNext("A")
                .expectError(ReactorException.class)
                .verify();
    }


    /**
     * Gives the visibility of which operator caused the problem without any performance overhead
     */
    @Test
    void OnErrorMap_reactorDebugAgent() {

        //given
        ReactorDebugAgent.init();
        ReactorDebugAgent.processExistingClasses();
        var e = new RuntimeException("Not a valid state");

        //when
        var flux = fluxAndMonoGeneratorService.OnErrorMap_checkpoint(e).log();

        //then
        StepVerifier.create(flux)
                .expectNext("A")
                .expectError(ReactorException.class)
                .verify();
    }

    @Test
    void doOnError() {
        //given
        RuntimeException e = new RuntimeException("Not a valid state");

        //when
        Flux<String> flux = fluxAndMonoGeneratorService.doOnError(e);

        //then
        StepVerifier.create(flux)
                .expectNext("A", "B", "C")
                .expectError(RuntimeException.class)
                .verify();
    }

    @Test
    void OnErrorContinue() {
        //when
        var flux = fluxAndMonoGeneratorService.OnErrorContinue().log();

        //then
        StepVerifier.create(flux)
                .expectNext("A", "C", "D")
                .verifyComplete();
    }


    @Test
    void exception_mono() {
        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_exception();

        //then
        StepVerifier.create(mono)
                .expectError(RuntimeException.class)
                .verify();

    }

    @Test
    void exception_mono_1() {
        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_exception();

        //then
        StepVerifier.create(mono)
                .expectErrorMessage("Exception Occurred")
                .verify();

    }

    @Test
    void exception_mono_onErrorResume() {

        //given
        var e = new IllegalStateException("Not a valid state");


        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_onErrorResume(e);

        //then
        StepVerifier.create(mono)
                .expectNext("abc")
                .verifyComplete();
    }

    @Test
    void exception_mono_onErrorReturn() {
        //when
        Mono<Object> mono = fluxAndMonoGeneratorService.exception_mono_onErrorReturn();

        //then
        StepVerifier.create(mono)
                .expectNext("abc")
                .verifyComplete();
    }

    @Test
    void exception_mono_onErrorMap() {

        //given
        var e = new IllegalStateException("Not a valid state");


        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_onErrorMap(e);

        //then
        StepVerifier.create(mono)
                .expectError(ReactorException.class)
                .verify();
    }

    @Test
    void exception_mono_onErrorContinue() {
        //given
        var input = "abc";

        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_onErrorContinue(input);

        //then
        StepVerifier.create(mono)
                .verifyComplete();
    }

    @Test
    void exception_mono_onErrorContinue_1() {

        //given
        var input = "reactor";

        //when
        var mono = fluxAndMonoGeneratorService.exception_mono_onErrorContinue(input);

        //then
        StepVerifier.create(mono)
                .expectNext(input)
                .verifyComplete();
    }

    @Test
    void generate() {
        //when
        var flux = fluxAndMonoGeneratorService.generate().log();

        //then
        StepVerifier.create(flux)
                .expectNext(2, 4, 6, 8, 10, 12, 14, 16, 18, 20)
                .verifyComplete();
    }


    @Test
    void create() {
        //when
        Flux<String> flux = fluxAndMonoGeneratorService.create_3().log();

        //then
        StepVerifier.create(flux)
                .expectNext("alex", "alex",
                        "ben", "ben",
                        "chloe", "chloe",
                        "alex",
                        "ben",
                        "chloe")
                .verifyComplete();
    }

    @Test
    void create_mono() {
        //when
        Mono<String> mono = fluxAndMonoGeneratorService.create_mono().log();

        //then
        StepVerifier.create(mono)
                .expectNext("alex")
                .verifyComplete();
    }

    @Test
    void push() {
        //when
        Flux<String> flux = fluxAndMonoGeneratorService.push().log();

        //then
        StepVerifier.create(flux)
                .expectNextCount(3)
                .verifyComplete();
    }

    @Test
    void handle() {
        //when
        Flux<String> flux = fluxAndMonoGeneratorService.handle().log();

        //then
        StepVerifier.create(flux)
                .expectNext("alex", "chloe")
                .verifyComplete();
    }


    @Test
    void mono_create() {
        //when
        var mono = fluxAndMonoGeneratorService.mono_create();

        //then
        StepVerifier.create(mono)
                .expectNext("abc")
                .verifyComplete();

    }

    @Test
    void namesFlux_flatmap_sequential() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_flatmap_sequential(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                //.expectNext("A", "L", "E", "X")
                .expectNextCount(9)
                .verifyComplete();

    }


    @Test
    void namesFlux_delay() {

        //given
        int stringLength = 3;

        //when
        var namesFlux = fluxAndMonoGeneratorService.namesFlux_delay(stringLength).log();

        //then
        StepVerifier.create(namesFlux)
                //.expectNext("ALEX", "BEN", "CHLOE")
                .expectNext("4-ALEX", "5-CHLOE")
                .verifyComplete();
    }

    @Test
    void range() {
        //when
        var rangeFlux = fluxAndMonoGeneratorService.range(5).log();

        //then
        StepVerifier.create(rangeFlux)
                //.expectNext(0,1,2,3,4)
                .expectNextCount(5)
                .verifyComplete();
    }
}