package com.reactive;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.ConnectableFlux;
import reactor.core.publisher.Flux;

import java.time.Duration;

import static com.reactive.util.Util.delay;

public class ColdAndHotPublisherTest {

    @Test
    public void coldPublisherTest() {

        Flux<Integer> flux = Flux.range(1, 10);

        flux.subscribe(s -> System.out.println("Subscriber 1 : " + s)); //emits the value from beginning
        flux.subscribe(s -> System.out.println("Subscriber 2 : " + s)); //emits the value from beginning
    }

    @Test
    public void hotPublisherTest() throws InterruptedException {

        Flux<Integer> stringFlux = Flux.range(1, 10)
                .delayElements(Duration.ofSeconds(1));

        ConnectableFlux<Integer> connectableFlux = stringFlux.publish();
        connectableFlux.connect();

        Thread.sleep(3000);
        // does not get the values from beginning because of the 3s delay
        connectableFlux.subscribe(s -> System.out.println("Subscriber 1 : " + s));

        Thread.sleep(5000);
        // starts reading later compared to subscriber 1 because it subscribes later
        connectableFlux.subscribe(s -> System.out.println("Subscriber 2 : " + s));
        Thread.sleep(10000);
    }

    @Test
    public void hotPublisherTest_autoConnect() throws InterruptedException {
        Flux<Integer> stringFlux = Flux.range(1, 10)
                .doOnSubscribe(s -> System.out.println("Subscription started"))
                .delayElements(Duration.ofSeconds(1));

        // this "autoConnect" call needs to be connected to the publish method itself
        var hotSource = stringFlux.publish().autoConnect(2);

        var disposable = hotSource.subscribe(s -> System.out.println("Subscriber 1 : " + s));
        delay(2000);

        var disposable1 = hotSource.subscribe(s -> System.out.println("Subscriber 2 : " + s));
        System.out.println("Two subscribers connected");
        delay(2000);

        disposable.dispose();
        disposable1.dispose();

        hotSource.subscribe(s -> System.out.println("Subscriber 3 : " + s)); // does not get the values from beginning
        Thread.sleep(10000);
    }

    @Test
    public void hotPublisherTest_refConnect() {

        Flux<Integer> stringFlux = Flux.range(1, 10)
                .doOnSubscribe(s -> System.out.println("Subscription received"))
                .doOnCancel(() -> System.out.println("Received Cancel Signal"))
                .delayElements(Duration.ofSeconds(1));

        // this "refCount" call needs to be connected to the publish method itself
        var hotSource = stringFlux
                .publish()
                .refCount(2);

        var disposable = hotSource.subscribe(s -> System.out.println("Subscriber 1 : " + s));
        delay(1000);

        // does not get the values from beginning
        var disposable1 = hotSource.subscribe(s -> System.out.println("Subscriber 2 : " + s));
        System.out.println("Two subscribers connected");
        delay(2000);

        // Remove the subscribers
        disposable.dispose();
        disposable1.dispose(); // this cancels the whole subscription

        //  This does not cause the publisher to emit the values, because it needs a minimum of 2 subscribers
        hotSource.subscribe(s -> System.out.println("Subscriber 3 : " + s));
        // Run by showing the above code and then enable the below code and run it.
        delay(2000);

        // By adding the fourth subscriber enables the minimum subscriber condition, and it starts to emit the values again (from the beginning)
        // hotSource.subscribe(s -> System.out.println("Subscriber 4: " + s));
        delay(10000);
    }
}
