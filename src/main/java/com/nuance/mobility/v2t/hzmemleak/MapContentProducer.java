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

import static com.hazelcast.core.Hazelcast.newHazelcastInstance;
import static java.util.concurrent.Executors.newScheduledThreadPool;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class MapContentProducer {
  
  private static final Logger LOGGER = Logger.getLogger(MapContentProducer.class.getName());
  
  private final BlockingQueue<ProducedItemInfo> producedItems;
  
  private final int messagesPerMinute;
  
  private final int nbMessagesToInject;
  
  private final boolean rotateMaps;
  
  private HazelcastInstance hz;
  
  private ScheduledExecutorService scheduler;
  
  private final AtomicInteger producerThreadCount = new AtomicInteger(); 
  
  private final AtomicInteger injectedMessageCount = new AtomicInteger();
  
  public MapContentProducer(BlockingQueue<ProducedItemInfo> producedItems, int messagesPerMinute, int nbMessagesToInject, boolean rotateMaps) {
    super();
    this.producedItems = producedItems;
    this.messagesPerMinute = messagesPerMinute;
    this.nbMessagesToInject = nbMessagesToInject;
    this.rotateMaps = rotateMaps;
  }

  public void init() {
    validate();
    initHazelcast();
    initScheduler();
    startInjection();
  }
  
  private void validate() {
    if (producedItems == null) throw new IllegalArgumentException("producedItems must not be null");
    if (messagesPerMinute <= 0) throw new IllegalArgumentException("messagesPerMinute must be > 0");
    if (nbMessagesToInject <= 0) throw new IllegalArgumentException("nbMessagesToInject must be > 0");
  }
  
  private void initHazelcast() {
    XmlConfigBuilder configBuilder = new XmlConfigBuilder(getClass().getResourceAsStream("/hz-config.xml"));
    hz = newHazelcastInstance(configBuilder.build());
  }
  
  private void initScheduler() {
    int corePoolSize = 8;
    LOGGER.info("Starting scheduler with corePoolSize=[" + corePoolSize + "] threads");
    scheduler = newScheduledThreadPool(corePoolSize, r -> new Thread(r, "producer-" + producerThreadCount.incrementAndGet()));
  }
  
  private void startInjection() {
    long delayMs = 60L * 1000L / messagesPerMinute;
    LOGGER.info("Using messagesPerMinue=[" + messagesPerMinute + "], delay between messages=[" + delayMs + "] ms");
    scheduler.scheduleAtFixedRate(this::submitInjection, 0, delayMs, MILLISECONDS);
  }
  
  private void submitInjection() {
    scheduler.submit(this::inject);
  }
  
  private void inject() {
    int msgIndex = injectedMessageCount.incrementAndGet();
    if (msgIndex > nbMessagesToInject) {
      LOGGER.info("Done injecting messages. Injected [" + msgIndex + "] messages.");
      return;
    }
    
    int mapIndex = rotateMaps ? msgIndex / messagesPerMinute : 0;
    boolean lastItemInMap = rotateMaps && ((msgIndex+1) % messagesPerMinute) == 0;
    
    String itemName = "item-" + msgIndex;
    String mapName = "map-" + mapIndex;
    String msgPayload = randomAlphanumeric(1024);
    
    IMap<String, String> map = hz.getMap(mapName);
    map.put(itemName, msgPayload);
    
    if (((msgIndex + 1) % 1000) == 0) {
      LOGGER.info("Injected [" + (msgIndex + 1) + "] messages so far, current map is [" + mapName + "]");
    }
    
    if (lastItemInMap) {
      LOGGER.info("Done producing items in map [" + mapName + "]");
    }

    final ProducedItemInfo producedItem = new ProducedItemInfo(mapName, itemName, lastItemInMap);
    // Push in queue at "expiration"
    scheduler.schedule(() -> {
      try {
        producedItems.put(producedItem);
      } catch (InterruptedException e) {
      }
    }, 60, TimeUnit.SECONDS);
  }
  
  
}

