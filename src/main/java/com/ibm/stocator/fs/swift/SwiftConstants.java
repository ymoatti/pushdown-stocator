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

package com.ibm.stocator.fs.swift;

import com.ibm.stocator.fs.common.Constants;

/**
 * Constants used in the Swift REST protocol.
 */
public class SwiftConstants {
  public static final String SWIFT_CONTAINER_PROPERTY = Constants.FS_SWIFT + ".CONTAINER-NAME";
  public static final String KEYSTONE_V3_AUTH = "keystoneV3";

  public static final String PUBLIC = ".public";
  public static final String SWIFT_PUBLIC_PROPERTY = Constants.FS_SWIFT + PUBLIC;

  public static final String AUTH_URL = ".auth.url";
  public static final String SWIFT_AUTH_PROPERTY = Constants.FS_SWIFT + AUTH_URL;

  public static final String TENANT = ".tenant";
  public static final String SWIFT_TENANT_PROPERTY = Constants.FS_SWIFT + TENANT;

  public static final String USERNAME = ".username";
  public static final String SWIFT_USERNAME_PROPERTY = Constants.FS_SWIFT + USERNAME;

  public static final String PASSWORD = ".password";
  public static final String SWIFT_PASSWORD_PROPERTY = Constants.FS_SWIFT + PASSWORD;

  public static final String REGION = ".region";
  public static final String SWIFT_REGION_PROPERTY = Constants.FS_SWIFT + REGION;

  public static final String USER_ID = ".userid";
  public static final String SWIFT_USER_ID_PROPERTY = Constants.FS_SWIFT + USER_ID;

  public static final String PROJECT_ID = ".projectid";
  public static final String SWIFT_PROJECT_ID_PROPERTY = Constants.FS_SWIFT + PROJECT_ID;

  public static final String AUTH_METHOD = ".auth.method";
  public static final String SWIFT_AUTH_METHOD_PROPERTY = Constants.FS_SWIFT + AUTH_METHOD;

  public static final String BLOCK_SIZE = ".block.size";
  public static final String SWIFT_BLOCK_SIZE_PROPERTY = Constants.FS_SWIFT + BLOCK_SIZE;

  public static final String MAX_RECORD_SIZE = ".max.record.size";
  public static final String SWIFT_MAX_RECORD_SIZE_PROPERTY = Constants.FS_SWIFT + MAX_RECORD_SIZE;

  public static final String FMODE_DELETE_TEMP_DATA = ".failure.mode.delete";
  public static final String FMODE_AUTOMATIC_DELETE_PROPERTY = Constants.FS_SWIFT
      + FMODE_DELETE_TEMP_DATA;

  public static final String SWIFT_BLOCK_SIZE_DEFAULT = "134217728";  // 128 MBytes
}
