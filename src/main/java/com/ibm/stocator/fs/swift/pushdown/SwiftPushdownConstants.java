package com.ibm.stocator.fs.swift.pushdown;

import java.util.Vector;

/**
 * Constants used for the pushdown mechanism for Swift
 */

public class SwiftPushdownConstants {
  public static final String SWIFT_PUSHDOWN_STORLET_NAME = "CSVStorlet-1.0.jar";
  public static final String SWIFT_PUSHDOWN_STORLET_HEADER_NAME = "X-Run-Storlet";
  public static final String SWIFT_PUSHDOWN_STORLET_PARAM_PREFIX = "X-Storlet-Parameter-";
  public static final String SWIFT_PUSHDOWN_STORLET_QUERY_SEPARATOR = ";";
  // separates parameter name and value in Spark string
  public static final String SWIFT_PUSHDOWN_STORLET_QUERY_PARAM_EQUAL = "=";
  // separates parameter name and value in storlet parameter
  public static final String SWIFT_STORLET_QUERY_PARAM_EQUAL = ":";
  public static final String SWIFT_STORLET_QUERY_START = "?";

  public static final Vector<String> SWIFT_PUSHDOWN_STORLET_QUERY_STRINGS = new Vector<String>();

  // This is the list of the possible fields that may be passed to the storlet
  public static final String PUSHDOWN_COLUMNS   = "selectedFields";
  public static final String PUSHDOWN_PREDICATE = "whereClause";

  static {
    SWIFT_PUSHDOWN_STORLET_QUERY_STRINGS.add(PUSHDOWN_COLUMNS);
    SWIFT_PUSHDOWN_STORLET_QUERY_STRINGS.add(PUSHDOWN_PREDICATE);
  }

}
