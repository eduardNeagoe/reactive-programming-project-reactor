package com.reactive.service;

import com.reactive.exception.ReactorException;
import lombok.extern.slf4j.Slf4j;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.function.Tuple8;

import java.time.Duration;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import static com.reactive.util.Util.delay;

@Slf4j
public class FluxAndMonoGeneratorService {


    public Flux<String> namesFlux() {
        var namesList = List.of("alex", "ben", "chloe");
        //return Flux.just("alex", "ben", "chloe");
        return Flux.fromIterable(namesList); // coming from a db or remote service

    }

    public Flux<String> namesFlux_immutability() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase);
    }


    public Flux<String> namesFlux_map(int stringLength) {
        var namesList = List.of("alex", "ben", "chloe");
        //return Flux.just("alex", "ben", "chloe");

        //Flux.empty()
        return Flux.fromIterable(namesList)
                //.map(s -> s.toUpperCase())
                .map(String::toUpperCase)
                .delayElements(Duration.ofMillis(500))
                .filter(s -> s.length() > stringLength)
                .map(s -> s.length() + "-" + s)
                .doOnNext(name -> {
                    System.out.println("name is : " + name);
                    name = name.toLowerCase();
                })
                .doOnSubscribe(s -> {
                    System.out.println("Subscription  is : " + s);
                })
                .doOnComplete(() -> {
                    System.out.println("Completed sending all the items.");
                })
                .doFinally((signalType) -> {
                    System.out.println("value is : " + signalType);
                })
                .defaultIfEmpty("default");
    }

    public Mono<String> namesMono() {

        return Mono.just("alex");

    }

    public Mono<String> namesMono_map_filter(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .defaultIfEmpty("default");

    }


    public Flux<String> namesFlux_flatmap(int stringLength) {
        List<String> namesList = List.of("alex", "ben", "chloe"); // a, l, e , x
        return Flux.fromIterable(namesList)
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                // ALEX,CHLOE -> A, L, E, X, C, H, L , O, E
                .flatMap(this::splitString);
    }

    public Flux<String> namesFlux_flatmap_async(int stringLength) {
        var namesList = List.of("alex", "ben", "chloe"); // a, l, e , x
        return Flux.fromIterable(namesList)
                //.map(s -> s.toUpperCase())
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitString_withDelay);


    }

    public Flux<String> namesFlux_concatmap(int stringLength) {
        // a, l, e , x
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .concatMap(this::splitString_withDelay);
    }

    public Mono<List<String>> namesMono_flatmap(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitStringMono); //Mono<List of A, L, E  X>
    }

    public Flux<String> namesMono_flatmapMany(int stringLength) {
        return Mono.just("alex")
                .map(String::toUpperCase)
                .flatMapMany(this::splitString_withDelay);
    }

    private Mono<List<String>> splitStringMono(String s) {
        var charArray = s.split("");
        return Mono.just(List.of(charArray))
                .delayElement(Duration.ofSeconds(1));
    }


    public Flux<String> namesFlux_transform(int stringLength) {

        Function<Flux<String>, Flux<String>> filterMap = name -> name.map(String::toUpperCase)
                .filter(s -> s.length() > stringLength);

        var namesList = List.of("alex", "ben", "chloe");
        return Flux.fromIterable(namesList)
                // transform gives you the opportunity to combine multiple operations using a single call.
                .transform(filterMap)
                .flatMap(this::splitString) // a, l, e , x, b, e, n, c, h, l, o, e
                .defaultIfEmpty("default");
    }


    public Flux<String> namesFlux_transform_switchIfEmpty(int stringLength) {

        Function<Flux<String>, Flux<String>> filterMap = name -> name.map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitString);

        Flux<String> defaultFlux = Flux.just("default")
                .transform(filterMap); //"D","E","F","A","U","L","T"

        List<String> namesList = List.of("alex", "ben", "chloe");
        return Flux.fromIterable(namesList)
                .transform(filterMap)
                .switchIfEmpty(defaultFlux);
        //using "map" would give the return type as Flux<Flux<String>

    }


    public Flux<String> namesFlux_transform_concatWith(int stringLength) {
        Function<Flux<String>, Flux<String>> filterMap = name -> name.map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .map(s -> s.length() + "-" + s);

        var namesList = List.of("alex", "ben", "chloe"); // a, l, e , x
        var flux1 = Flux.fromIterable(namesList)
                .transform(filterMap);

        return flux1.concatWith(Flux.just("anna")
                .transform(filterMap));

    }

    public Mono<String> name_defaultIfEmpty() {

        return Mono.<String>empty() // db or rest call
                .defaultIfEmpty("Default");

    }


    public Mono<String> name_switchIfEmpty() {

        Mono<String> defaultMono = Mono.just("Default");
        return Mono.<String>empty() // db or rest call
                .switchIfEmpty(defaultMono);

    }

    // "A", "B", "C", "D", "E", "F"
    public Flux<String> concat() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");

        return Flux.concat(abcFlux, defFlux);
    }


    // "A", "B", "C", "D", "E", "F"
    public Flux<String> concatWith() {

        var abcFlux = Flux.just("A", "B", "C");

        var defFlux = Flux.just("D", "E", "F");

        return abcFlux.concatWith(defFlux).log();


    }

    public Flux<String> concatWith_mono() {
        Mono<String> aMono = Mono.just("A");
        Flux<String> bMono = Flux.just("B");

        return aMono.concatWith(bMono);
    }

    // "A", "D", "B", "E", "C", "F"
    // Flux is subscribed early
    public Flux<String> merge() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(100));

        Flux<String> defFlux = Flux.just("D", "E", "F")
                .delayElements(Duration.ofMillis(125));

        return Flux.merge(abcFlux, defFlux).log();
    }

    // "A", "D", "B", "E", "C", "F"
    // Flux is subscribed early
    public Flux<String> mergeWith() {

        var abcFlux = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(100));

        var defFlux = Flux.just("D", "E", "F")
                .delayElements(Duration.ofMillis(125));

        return abcFlux.mergeWith(defFlux).log();


    }

    public Flux<String> mergeWith_mono() {
        Mono<String> aMono = Mono.just("A");
        Flux<String> bMono = Flux.just("B");

        return aMono.mergeWith(bMono);
    }

    // "A","B","C","D","E","F"
    // Flux is subscribed early
    public Flux<String> mergeSequential() {
        Flux<String> abcFlux = Flux.just("A", "B", "C")
                .delayElements(Duration.ofMillis(100));

        Flux<String> defFlux = Flux.just("D", "E", "F")
                .delayElements(Duration.ofMillis(150));

        return Flux.mergeSequential(abcFlux, defFlux).log();
    }

    // AD, BE, FC
    public Flux<String> zip() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");

        return Flux.zip(abcFlux, defFlux, (first, second) -> first + second);
    }


    public Flux<String> zip_1() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");
        Flux<String> flux123 = Flux.just("1", "2", "3");
        Flux<String> flux456 = Flux.just("4", "5", "6");

        // AD14, BE25, CF36
        return Flux.zip(abcFlux, defFlux, flux123, flux456)
                .map(t4 -> t4.getT1() + t4.getT2() + t4.getT3() + t4.getT4());
    }

    public Flux<String> zip_Mono() {
        Mono<String> aMono = Mono.just("A");
        Mono<String> bMono = Mono.just("B");

        return Flux.zip(aMono, bMono, (first, second) -> first + second);
    }

    // AD, BE, CF
    public Flux<String> zipWith() {
        Flux<String> abcFlux = Flux.just("A", "B", "C");
        Flux<String> defFlux = Flux.just("D", "E", "F");

        return abcFlux.zipWith(defFlux, (first, second) -> first + second);
    }

    public Mono<String> zipWith_mono() {
        Mono<String> aMono = Mono.just("A");
        Mono<String> bMono = Mono.just("B");

        return aMono.zipWith(bMono)
                .map(t2 -> t2.getT1() + t2.getT2());
    }

    public Mono<Tuple8<Object, String, Object, Object, String, Object, Object, String>> zipWith_mono_delay() {

        var aMono = Mono.just("A");

        var bMono = Mono.just("B");


        return Mono.zipDelayError(
                        Mono.error(new RuntimeException("Reason 1")),
                        Mono.just("ok"),
                        Mono.error(new RuntimeException("Reason 2")),
                        Mono.error(new RuntimeException("Reason 3")),
                        Mono.just("ok"),
                        Mono.error(new RuntimeException("Reason 4")),
                        Mono.error(new RuntimeException("Reason 5")),
                        Mono.just("ok")
                )
                .onErrorMap(e -> {
                            System.out.println("exception : " + e);
                            return Exceptions.unwrapMultiple(e).stream()
                                    .reduce((e1, e2) -> new RuntimeException(String.join(", ", e1.getMessage(), e2.getMessage()))).get();
                        }
                );
    }

    public Flux<String> exception_flux() {
        return Flux.just("A", "B", "C")
                .concatWith(Flux.error(new RuntimeException("Exception Occurred")))
                .concatWith(Flux.just("D"));
    }


    /**
     * This provides a single fallback value
     */
    public Flux<String> OnErrorReturn() {
        return Flux.just("A", "B", "C")
                .concatWith(Flux.error(new IllegalStateException("Exception Occurred")))
                .onErrorReturn("D");
    }

    /**
     * This provides a fallback value as a Reactive Stream
     */
    public Flux<String> OnErrorResume(Exception e) {
        return Flux.just("A", "B", "C")
                .concatWith(Flux.error(e))
                .onErrorResume((exception) -> {
                    log.error("Exception is ", exception);
                    if (exception instanceof IllegalStateException)
                        return Flux.just("D", "E", "F");
                    else
                        return Flux.error(exception);
                });
    }

    /**
     * This helps to drop elements causing the issue and move on with the other elements
     */
    public Flux<String> OnErrorContinue() {
        return Flux.just("A", "B", "C")
                .map(name -> {
                    if (name.equals("B")) {
                        throw new IllegalStateException("Exception Occurred");
                    }
                    return name;
                })
                .concatWith(Flux.just("D"))
                .onErrorContinue((exception, value) -> {
                    log.error("Exception is : " + exception);
                    log.info("Value is : " + value);
                });
    }


    /**
     * Used to transform the error from one type to another
     */
    public Flux<String> OnErrorMap() {
        return Flux.just("A", "B", "C")
                .map(name -> {
                    if (name.equals("B")) {
                        throw new IllegalStateException("Exception Occurred");
                    }
                    return name;
                })
                .onErrorMap((exception) -> {
                    // business exception
                    return new ReactorException(exception, exception.getMessage());
                });
    }


    /**
     * Used to transform the error from one type to another
     */
    public Flux<String> OnErrorMap_checkpoint(Exception e) {
        return Flux.just("A")
                .concatWith(Flux.error(e))
                // marking a checkpoint
                .checkpoint("errorSpot")
                .onErrorMap((exception) -> {
                    log.error("Exception is : ", exception);
                    return new ReactorException(exception, exception.getMessage());
                });
    }

    public Flux<String> OnErrorMap_2(Exception e) {
        return Flux.just("A")
                .concatWith(Flux.error(e))
                .onErrorMap((exception) -> {
                    log.error("Exception is : ", exception);
                    return new ReactorException(exception, exception.getMessage());
                });
    }

    public Flux<String> doOnError(Exception e) {
        return Flux.just("A", "B", "C")
                .concatWith(Flux.error(e))
                .doOnError((exception) -> {
                    System.out.println("Exception is : " + e);
                    //Write any logic you would like to perform when an exception happens
                });
    }


    public Mono<Object> exception_mono_exception() {
        return Mono.just("B")
                .map(value -> {
                    throw new RuntimeException("Exception Occurred");
                });
    }


    /**
     * This operator can be used to provide a default value when an error occurs
     */
    public Mono<Object> exception_mono_onErrorReturn() {
        return Mono.just("B")
                .map(value -> {
                    throw new RuntimeException("Exception Occurred");
                }).onErrorReturn("abc");
    }

    /**
     *  This operator can be used to resume from an exception.
     *  The recovery value here will be a Mono instead of the direct value
     */
    public Mono<Object> exception_mono_onErrorResume(Exception e) {

        var mono = Mono.error(e);

        return mono.onErrorResume((ex) -> {
            System.out.println("Exception is " + ex);
            if (ex instanceof IllegalStateException)
                return Mono.just("abc");
            else
                return Mono.error(ex);
        });
    }

    /**
     * This operator can be used to map the exception to another user defined or custom exception
     */
    public Mono<Object> exception_mono_onErrorMap(Exception e) {

        return Mono.just("B")
                .map(value -> {
                    throw new RuntimeException("Exception Occurred");
                }).onErrorMap(ex -> {
                    System.out.println("Exception is " + ex);
                    return new ReactorException(ex, ex.getMessage());
                });
    }

    /**
     * This operator allows the reactive stream to continue emitting elements when an error occurred in the flow
     */
    public Mono<String> exception_mono_onErrorContinue(String input) {
        return Mono.just(input)
                .map(data -> {
                    if (data.equals("abc")) {
                        throw new RuntimeException("Exception Occurred");
                    } else {
                        return data;
                    }
                })
                .onErrorContinue((ex, val) -> {
                    log.error("Exception is " + ex);
                    log.error("Value that caused the exception is " + val);
                });
    }


    public Flux<Integer> generate() {
        return Flux.generate(
                // state supplier - the starting point
                // called for each incoming Subscriber to provide the initial state
                () -> 1,
                // BiFunction that generates a single signal on each pass and return a (new) state
                (state, sink) -> {
                    // emit the current value times 2
                    sink.next(state * 2);
                    // stop when the current value is 10
                    if (state == 10) {
                        sink.complete();
                    }
                    // increment the current value to continue generating events
                    return state + 1;
                });
    }

    public static List<String> names() {
        delay(1000);
        return List.of("alex", "ben", "chloe");
    }

    public Flux<String> create() {

        return Flux.create(sink -> {
                    names().forEach(t -> sink.next(t));
                    // signal completion, otherwise the processing will run forever
                    // terminates the sequence successfully, generating an onComplete signal
                    sink.complete();
                },
                FluxSink.OverflowStrategy.BUFFER
        );
    }

    public Flux<String> create_2() {
        return Flux.create(sink -> {
                    // releases the calling thread by delegating this execution to another thread
                    // place the blocking call inside the create function
                    CompletableFuture.supplyAsync(() -> names())
                            .thenAccept(names -> names.forEach(t -> sink.next(t)))
                            .thenRun(() -> sendEvents(sink))
                            .whenComplete((data, exception) -> sink.error(exception));
                },
                FluxSink.OverflowStrategy.BUFFER
        );
    }

    public Flux<String> create_3() {
        return Flux.create(sink -> {
                    // releases the calling thread by delegating this execution to another thread
                    // place the blocking call inside the create function
                    CompletableFuture.supplyAsync(() -> names())
                            .thenAccept(names -> names.forEach(t -> {
                                sink.next(t);
                                //  multiple emissions in a single run
                                //  we can emit multiple events here
                                sink.next(t);
                            }))
                            .thenRun(() -> sendEvents(sink))
                            .whenComplete((data, exception) -> sink.error(exception));
                },
                FluxSink.OverflowStrategy.BUFFER
        );
    }

    public Mono<String> create_mono() {
        return Mono.create(sink ->
                CompletableFuture.supplyAsync(this::name)
                        .thenAccept(sink::success));
    }

    // blocking operation - because of the delay
    private String name() {
        delay(1000);
        return "alex";
    }

    public Flux<String> push() {
        return Flux.push(
                // place the blocking call inside the push function
                sink -> CompletableFuture.supplyAsync(() -> names())
                        .thenAccept(names -> names.forEach((s) -> sink.next(s)))
                        .thenRun(() -> sendEvents(sink))
                        .whenComplete((data, exception) -> sink.error(exception))
        );
    }

    public Flux<String> handle() {
        return Flux.fromIterable(List.of("alex", "ben", "chloe"))
                .handle((name, sink) -> {

                    if (name.length() > 3) {
                        sink.next(name);
                    }

                });
    }


    public Mono<String> mono_create() {
        return Mono.create(sink -> {
            delay(1000);
            sink.success("abc");
        });
    }

    public void sendEvents(FluxSink<String> sink) {
        CompletableFuture.supplyAsync(() -> names()) // place the blocking call inside the create function
                .thenAccept(names -> names.forEach((s) -> sink.next(s)))
                // send the completion signal in thread that has been delegated to process this future
                .thenRun(() -> sink.complete());
    }

    /**
     * ALEX -> FLux(A, L, E, X)
     */
    private Flux<String> splitString(String name) {
        var charArray = name.split("");
        return Flux.fromArray(charArray);
    }

    private Flux<String> splitString_withDelay(String name) {
        int delay = new Random().nextInt(1000);
        String[] charArray = name.split("");
        return Flux.fromArray(charArray)
                .delayElements(Duration.ofMillis(delay));
    }

    private Flux<String> delayString(String string) {

        var delay = new Random().nextInt(1000);
        return Flux.just(string)
                .delayElements(Duration.ofMillis(delay));
    }

    /**
     * @return AL, EX, CH, LO, E
     */
    public Flux<String> namesFlux_flatmap_sequential(int stringLength) {
        var namesList = List.of("alex", "ben", "chloe"); // a, l, e , x
        return Flux.fromIterable(namesList)
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .flatMap(this::splitString)
                .log();
    }

    private Flux<String> lowercase(Flux<String> stringFlux) {
        delay(1000);
        return stringFlux.map(String::toLowerCase);
    }


    public Flux<String> namesFlux_delay(int stringLength) {
        var namesList = List.of("alex", "ben", "chloe");

        return Flux.fromIterable(namesList)
                .delayElements(Duration.ofSeconds(1))
                .map(String::toUpperCase)
                .filter(s -> s.length() > stringLength)
                .map(s -> s.length() + "-" + s);
    }

    public Flux<Integer> range(int max) {

        return Flux.range(0, max);
    }


    public Flux<Integer> generateLongFlux(int maxNum) {

        return Flux.range(0, maxNum);
    }

    public Flux<Integer> namesFlux1() {

        return Flux.fromIterable(List.of("Alex", "ben", "chloe"))
                .map(String::length)
                /*.publishOn(Schedulers.boundedElastic())
                .map(length -> {
                    delay(1000);
                    return length;
                })*/
                .log();
    }

    public Flux<Integer> namesFlux_subscribeOn() {

        return Flux.fromIterable(List.of("Alex", "ben", "chloe"))
                .map(String::length)
                .subscribeOn(Schedulers.boundedElastic())
                .map(length -> {
                    delay(1000);
                    return length;
                })
                .log();
    }

    public Flux<Integer> namesFlux_subscribeOn_publishOn() {

        Scheduler s = Schedulers.newParallel("parallel-scheduler", 4);

        return Flux.fromIterable(List.of("Alex", "ben", "chloe"))
                .map(name -> {
                    log.info("inside first map");
                    return name.length();
                })
                .subscribeOn(Schedulers.boundedElastic())
                .map(length -> {
                    log.info("inside second map");
                    return length;
                })
                .publishOn(s)
                .map(length -> {
                    log.info("inside third map");
                    delay(1000);
                    return length;
                })
                .log();
    }

    public Flux<Integer> generateLongFlux_withException(int maxNum) {

        return Flux.range(0, maxNum)
                .map(i -> {
                    if (i == 5) {
                        throw new IllegalStateException("Not allowed for number : " + i);
                    }
                    return i;
                });
    }

    public Flux<Integer> generateLongFlux_withException_checkpoint(int maxNum) {

        return Flux.range(0, maxNum)
                .map(i -> {
                    if (i == 5) {
                        throw new IllegalStateException("Not allowed for number : " + i);
                    }
                    return i;
                })
                .checkpoint("flux_error", true);
    }

    public Flux<Integer> generateLongFlux_withDelay(int maxNum) {

        return Flux.range(0, maxNum)
                .delayElements(Duration.ofSeconds(1));
    }

    public static void main(String[] args) {

        FluxAndMonoGeneratorService fluxAndMonoGeneratorService = new FluxAndMonoGeneratorService();

        Flux<String> namesFlux = fluxAndMonoGeneratorService.namesFlux().log();

        namesFlux.subscribe((name) -> {
            System.out.println("Name is : " + name);
        });

        Mono<String> namesMono = fluxAndMonoGeneratorService.namesMono().log();

        namesMono.subscribe((name) -> {
            System.out.println("Name is : " + name);
        });
    }


}
