/**
 * (C) Copyright IBM Corp. 2015, 2016
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.ibm.stocator.fs.common;

/**
 * General constants used in the code
 */
public class Constants {

  /*
   * Swift name space identifier
   */
  public static final String SWIFT = "swift2d";
  /*
   * Swift configuration prefix in the core-site.xml
   */
  public static final String FS_SWIFT = "fs." + SWIFT;
  /*
   * Swift configuration key in the core-site.xml
   */
  public static final String SWIFT_SERVICE_PREFIX = FS_SWIFT + ".service.";
  /*
   * Hadoop identification for the temporary directory
   */
  public static final String HADOOP_TEMPORARY = "_temporary";
  /*
   * Hadoop identification for TASK_ID attempt
   */
  public static final String HADOOP_ATTEMPT = "attempt_";
  /*
   * Hadoop _SUCCESS object
   */
  public static final String HADOOP_SUCCESS = "_SUCCESS";
}
