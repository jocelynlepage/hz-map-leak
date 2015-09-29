/* 
 * ---------------------------------------------------------------------------
 *
 * COPYRIGHT (c) 2015 Nuance Communications Inc. All Rights Reserved.
 *
 * The copyright to the computer program(s) herein is the property of
 * Nuance Communications Inc. The program(s) may be used and/or copied
 * only with the written permission from Nuance Communications Inc.
 * or in accordance with the terms and conditions stipulated in the
 * agreement/contract under which the program(s) have been supplied.
 *
 * Author: rael
 * Date  : Sep 29, 2015
 *
 * ---------------------------------------------------------------------------
 */
package com.nuance.mobility.v2t.hzmemleak;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class Main {

  public static void main(String[] args) {
    BlockingQueue<ProducedItemInfo> queue = new ArrayBlockingQueue<>(10000);
    
    int messagesPerMinute = 10000;
    int nbMessages = 14400000;
    boolean rotateMaps = true;
    
    MapContentProducer producer = new MapContentProducer(queue, messagesPerMinute, nbMessages, rotateMaps);
    producer.init();
    
    MapContentConsumer consumer = new MapContentConsumer(queue);
    consumer.init();
        
  }
  
}

