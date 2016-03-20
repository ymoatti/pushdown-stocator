/**
 * (C) Copyright IBM Corp. 2015, 2016
 * <p/>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.ibm.stocator.fs.swift.pushdown;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.javaswift.joss.model.Container;
import org.javaswift.joss.headers.GeneralHeader;
import org.javaswift.joss.instructions.DownloadInstructions;
import org.javaswift.joss.headers.object.range.AbstractRange;

import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.ibm.stocator.fs.swift.SwiftAPIClient;
import com.ibm.stocator.fs.swift.SwiftInputStream;

public class SwiftPushdownInputStream extends SwiftInputStream {

  private static final Logger LOG = LoggerFactory.getLogger(SwiftPushdownInputStream.class);

  private final List<GeneralHeader> pushdownHeaders;

  public SwiftPushdownInputStream(SwiftAPIClient storeNative,
                                  String hostName, Path path)
    throws IOException {
    super(storeNative);
    LOG.debug("init: {}", path.toString());
    Container theContainer = nativeStore.getAccount().getContainer(nativeStore.getDataRoot());
    // handlePushdownHeaders does the following:
    // 1. put into pushdownHeaders all the necessary headers
    // 2. returns a path equals to input path but where the query string was truncated
    pushdownHeaders = new ArrayList<>();
    String noQueryPath = handlePushdownHeaders(path, theContainer, pushdownHeaders);
    String objectName = noQueryPath.substring(hostName.length());
    storedObject = theContainer.getObject(objectName);

    if (!storedObject.exists()) {
      throw new FileNotFoundException(objectName + " does not exists");
    }
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    super.seekPart1(targetPos);

    DownloadInstructions instructions = new DownloadInstructions();

    // add the Pushdown headers:
    for (GeneralHeader nextHeader : pushdownHeaders) {
      instructions.addHeader(nextHeader);
    }

    LOG.warn("pushdownHeaders.size is  " + pushdownHeaders.size());
    if (pushdownHeaders.size() <= 0) {
      instructions = super.seekPart2(targetPos);
    } else {
      AbstractRange range;
      range = new AbstractRange(targetPos, targetPos + nativeStore.getBlockSize()) {

          @Override
          public long getTo(int arg0) {
            return offset;
          }

          @Override
          public long getFrom(int arg0) {
            return length;
          }

          @Override
          public String getHeaderName() {
            return "X-Storlet-Range";
          }

        };
      instructions.setRange(range);
    }

    super.seekPart3(targetPos, instructions);
  }

  /**
   * @param path
   * @return true if path contains a query part that necessitates pushdown storlet invocation
   */
  private String handlePushdownHeaders(Path path, Container theContainer,
                                       List<GeneralHeader> pushdownHeaders) {
    String fullPath = path.toString();

    int queryStartIndex = fullPath.indexOf(SwiftPushdownConstants.SWIFT_STORLET_QUERY_START);
    if (queryStartIndex < 0) {
      LOG.debug("No pushdown since " + SwiftPushdownConstants.SWIFT_STORLET_QUERY_START
          + " not to be found in full path " + fullPath);
      return fullPath;
    }

    String queryPath = fullPath.substring(queryStartIndex + 1);

    String[] theParams =
      queryPath.split(SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_QUERY_SEPARATOR);  // ;
    int addedQueryParams = 0;

    for (String nextParam : theParams) {
      String[] paramParts = nextParam.split(
              SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_QUERY_PARAM_EQUAL);
      if (paramParts.length != 2) {
        LOG.warn("Skipping bad formed Storlet query parameter " + nextParam);
      } else {
        String paramKey =
            SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_PARAM_PREFIX
            + addedQueryParams++;
        String key = paramParts[0];
        String val = paramParts[1];
        if (!isExpectedPushdownStorletParameter(key)) {
          LOG.warn("Skipping unexpected Pushdown Storlet parameter key " + key);
        } else {
          String theVal = paramTransform(key, val);
          String paramValue = key
              + SwiftPushdownConstants.SWIFT_STORLET_QUERY_PARAM_EQUAL
              + theVal;

          pushdownHeaders.add(new GeneralHeader(paramKey.trim(), paramValue.trim()));
        }
      }
    }

    if (addedQueryParams > 0) {
      pushdownHeaders.add(new GeneralHeader(
          SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_HEADER_NAME,
          SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_NAME)
      );
      LOG.warn("Number of added ##Headers to container is " + pushdownHeaders.size());
    }

    return fullPath.substring(0, queryStartIndex);
  }

  private String paramTransform(String paramKey, String paramVal) {
    if (SwiftPushdownConstants.PUSHDOWN_COLUMNS.equals(paramKey)) {
      return paramVal.replace(":", "_");
    } else {
      return paramVal;
    }
  }

  private boolean isExpectedPushdownStorletParameter(String theKey) {
    for (String nextKey : SwiftPushdownConstants.SWIFT_PUSHDOWN_STORLET_QUERY_STRINGS) {
      if (nextKey.equals(theKey)) {
        return true;
      }
    }
    return false;
  }

}
