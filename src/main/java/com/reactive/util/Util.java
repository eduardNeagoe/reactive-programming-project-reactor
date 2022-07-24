package com.reactive.util;

import static java.lang.Thread.sleep;

public class Util {

    public static void delay(int ms) {
        try {
            sleep(ms);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }
}
