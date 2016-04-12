package com.ibm.stocator.fs.swift.pushdown;
// package com.ibm.pushdown.sql.csv;

/*
 * WARNING:  this file was copied from com.ibm.stocator.fs.swift.pushdown package
 *           and both should be identical so that strings used for parameter keys are
 *           the same!
 */

/**
 * Constants which are specific to the SQL CSV pushdown mechanism.
 * These constants are both used in the stocator and the Storlet code itself
 *
 * Beware:  changing any of these constants will necessitate to update the CSV Storlet code!
 */

public interface PushdownStorletConstants {

  public static final String SWIFT_PUSHDOWN_STORLET_NAME = "CSVStorlet-1.0.jar";
  public static final String SWIFT_PUSHDOWN_STORLET_HEADER_NAME = "X-Run-Storlet";
  public static final String SWIFT_PUSHDOWN_STORLET_PARAM_PREFIX = "X-Storlet-Parameter-";
  public static final String SWIFT_PUSHDOWN_STORLET_QUERY_SEPARATOR = ";";

  // separates parameter name and value in Spark string
  public static final String SWIFT_PUSHDOWN_STORLET_QUERY_PARAM_EQUAL = "=";

  // separates parameter name and value in storlet parameter
  public static final String SWIFT_STORLET_QUERY_PARAM_EQUAL = ":";
  public static final String SWIFT_STORLET_QUERY_START = "?";

  /**
   * permits to the client to specify the buffer length
   */
  public static final String SWIFT_PUSHDOWN_STORLET_STREAM_BUFFER_LENGTH =
                  "X-Storlet-StreamBufferLength";
  public static final String SWIFT_PUSHDOWN_STORLET_RANGE =
                  "X-Storlet-Range";
  public static final String SWIFT_PUSHDOWN_STORLET_REQUESTED_RANGE =
                  "X-Storlet-Requested-Range";
  public static final String SWIFT_PUSHDOWN_STORLET_BLOCK_SIZE =
                  "X-Storlet-BlockSize";
  public static final String SWIFT_PUSHDOWN_STORLET_ADDED_PREFIX_LENGTH =
                  "X-Storlet-AddedPrefixLength";
  public static final String SWIFT_PUSHDOWN_STORLET_FILE_ENCRYPTION =
                  "X-Storlet-FileEncryption";
  public static final String SWIFT_PUSHDOWN_STORLET_RECORD_DELIMITER =
                  "X-Storlet-RecordDelimiter";

  public static final String SWIFT_PUSHDOWN_STORLET_REQUESTED_RANGE_SEPARATOR = "_";

  // This is the list of the possible fields that may be passed to the storlet
  public static final String PUSHDOWN_COLUMNS   = "selectedFields";
  public static final String PUSHDOWN_PREDICATE = "whereClause";

  public static final String DEFAULT_RECORD_DELIMITER = "\n";
  public static final String DEFAULT_PREDICATE = "";
  public static final String DEFAULT_COLUMNS = "";
  public static final String DEFAULT_FILE_ENCRYPTION = "UTF-8";
  public static final long   DEFAULT_STREAM_BUFFER_LENGTH = 64 * 1024;  // 64 K

  public static final String COLUMNS_SEPARATOR = "_";   // in fact used in upper packages
}
