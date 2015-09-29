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

public class ProducedItemInfo {
  public final String mapName;
  public final String itemName;
  public final boolean lastItemInMap;
  
  public ProducedItemInfo(String mapName, String itemName, boolean lastItemInMap) {
    super();
    this.mapName = mapName;
    this.itemName = itemName;
    this.lastItemInMap = lastItemInMap;
  }
}

