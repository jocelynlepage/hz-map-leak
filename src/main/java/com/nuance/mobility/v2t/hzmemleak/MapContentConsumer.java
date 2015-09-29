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

import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Logger;
import java.util.stream.IntStream;

import com.hazelcast.config.XmlConfigBuilder;
import com.hazelcast.core.HazelcastInstance;
import com.hazelcast.core.IMap;

public class MapContentConsumer {

  private static final Logger LOGGER = Logger.getLogger(MapContentConsumer.class.getName());
  
  private BlockingQueue<ProducedItemInfo> producedItems;
  
  private HazelcastInstance hz;
  
  private final AtomicInteger removedItemsCount = new AtomicInteger();
  
  private final Set<String> destroyedMapNames = new HashSet<>();
  
  public MapContentConsumer(BlockingQueue<ProducedItemInfo> producedItems) {
    super();
    this.producedItems = producedItems;
  }
  
  public void init() {
    validate();
    initHazelcast();
    initConsumers();
  }
  
  private void validate() {
    if (producedItems == null) throw new IllegalArgumentException("producedItems must not be null");
  }
  
  private void initHazelcast() {
    XmlConfigBuilder configBuilder = new XmlConfigBuilder(getClass().getResourceAsStream("/hz-config.xml"));
    hz = newHazelcastInstance(configBuilder.build());
  }
  
  private void initConsumers() {
    IntStream
      .range(0, 8)
      .forEach(i -> {
        new Thread(this::consume, "Consumer-" + (i+1)).start();
      });
  }
  
  private void consume() {
    try {
      while (true) {
        ProducedItemInfo item = producedItems.take();
        boolean mapDestroyed = isMapDestroyed(item.mapName, item.lastItemInMap);
        if (mapDestroyed) {
          LOGGER.info("Skipping item [" + item.itemName + "] as map [" + item.mapName + "] is destroyed (or about to be destroyed)");
          continue;
        }
        
        IMap<String, String> map = hz.getMap(item.mapName);

        int removeAttemptCount = 0;
        while (true) {
          if (removeAttemptCount == 5) {
            LOGGER.warning("Couldn't remove item [" + item.itemName + "] from map [" + item.mapName + "] after 5 attempts");
            break;
          }
          
          String removed = map.remove(item.itemName);
          if (removed == null) {
            Thread.sleep(1000L);
            removeAttemptCount++;
            continue;
          }
          
          int removedCount = removedItemsCount.incrementAndGet();
          if ((removedCount % 1000) == 0) {
            LOGGER.info("Removed [" + removedCount + "] items so far");
          }
          break;
        }
        
        if (item.lastItemInMap) {
          map.destroy();
          LOGGER.info("Destroyed map [" + item.mapName + "]");
        }
      }
    } catch (InterruptedException e) {
    }
  }
  
  private synchronized boolean isMapDestroyed(String mapName, boolean addToDestroyed) {
    boolean destroyed = destroyedMapNames.contains(mapName);
    if (addToDestroyed) {
      destroyedMapNames.add(mapName);
    }
    return destroyed;
  }
  
}

