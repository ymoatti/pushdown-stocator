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

  private final List<GeneralHeader> pushdownHeaders = new ArrayList<>();
  private final long blockSize;   // used by stocator when reading a swift object
  private final String csvRecordDelimiter;
  private final int delimiterLength;
  private final long maxRecordSize;  // maximum length in bytes of a CSV record

  public SwiftPushdownInputStream(SwiftAPIClient storeNative,
                                  String hostName, Path path)
    throws IOException {
    super(storeNative);
    LOG.debug("init: {}", path.toString());
    String str = " SwiftPushdownInputStream.constructor storeNative " + storeNative
        + " hostName = " + hostName + " path = " + path;
    // SwiftInputStream.printStackTrace(str);
    LOG.debug(str);
    Container theContainer = nativeStore.getAccount().getContainer(nativeStore.getDataRoot());
    blockSize = nativeStore.getBlockSize();
    csvRecordDelimiter = nativeStore.getCsvRecordDelimiter();
    delimiterLength = csvRecordDelimiter.length();
    maxRecordSize = nativeStore.getMaxRecordSize();

    // handlePushdownHeaders does the following:
    // 1. put into pushdownHeaders all the necessary headers, except for the prefixLength
    //    which can not be determined till seek is invoked
    // 2. returns a path equals to input path but where the query string was truncated
    String noQueryPath = handlePushdownHeaders(path, theContainer, pushdownHeaders);
    String objectName = noQueryPath.substring(hostName.length());

    storedObject = theContainer.getObject(objectName);
    if (!storedObject.exists()) {
      throw new FileNotFoundException(objectName + " does not exists");
    } else {
      LOG.debug("SwiftPushdownInputStream.constructor blockSize = " + blockSize
               + " csvRecordDelimiter = " + csvRecordDelimiter
               + " delimiterLength = " + delimiterLength);
    }
  }

  @Override
  public synchronized void seek(long targetPos) throws IOException {
    // printStackTrace("#### seek " + targetPos);
    LOG.debug("#### seek " + targetPos);
    super.seekPart1(targetPos);

    DownloadInstructions instructions = new DownloadInstructions();

    LOG.debug("pushdownHeaders.size is  " + pushdownHeaders.size());
    if (pushdownHeaders.size() <= 0) {

      instructions = super.seekPart2(targetPos);
    } else {  // SQL pushdown case:
      // Compute offset and length to fix the problem of the broken first/last records:
      // computeStorletOffset will add the prefix header (if needed)
      long modifiedFrom = computeStorletOffset(targetPos,
                 delimiterLength,
                 blockSize,
                 pushdownHeaders);
      long modifiedTo = targetPos + nativeStore.getBlockSize() + maxRecordSize;

      String requestedRange = (new Long(targetPos))
          + PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_REQUESTED_RANGE_SEPARATOR
          + new Long(modifiedTo);
      addStorletParameterHeader(pushdownHeaders,
                 PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_REQUESTED_RANGE,
                 requestedRange);

      // add all the pushdown headers:
      for (GeneralHeader nextHeader : pushdownHeaders) {
        instructions.addHeader(nextHeader);
      }

      LOG.debug("Range from byte = " + modifiedFrom + " and till byte  = " +  modifiedTo);
      AbstractRange range = new AbstractRange(modifiedFrom, modifiedTo) {

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
            // "X-Storlet-Range"
            return PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_RANGE;
          }

        };
      instructions.setRange(range);
    }

    super.seekPart3(targetPos, instructions);
  }

  private long computeStorletOffset(final long targetPos, final int delimiterLength,
           final long blockSize, final List<GeneralHeader> pushdownHeaders) {

    // First add to the headers the blocksize to enable the storlet not to produce duplicates
    {
      LOG.debug(" computeStorletOffset targetPos = " + targetPos + " delimiterLength = "
               + delimiterLength + " blockSize = " + blockSize + " pushdownHeaders.size() = "
               + pushdownHeaders.size());
      addStorletParameterHeader(pushdownHeaders,
                PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_BLOCK_SIZE,
                Long.toString(blockSize));
    }

    if (targetPos == 0) {  // Attempting to read first slice of the object
      LOG.debug("computeStorletOffset handling first partition at offset = " + targetPos);
      return targetPos;
    } else {
      // this is not the first slice, we have to start a bit earlier to make sure that
      // all read bytes till first encountered end of record can be discarded
      long retVal = targetPos - delimiterLength;
      LOG.debug("computeStorletOffset handling NON first partition offset = " + retVal);

      // Add now the ADDED_PREFIX_LENGTH: needed by the CSV pushdown storlet
      // to know that all read bytes till first encountered end of record should be discarded
      addStorletParameterHeader(pushdownHeaders,
                PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_ADDED_PREFIX_LENGTH,
                Integer.toString(delimiterLength));

      return retVal;
    }
  }

  /**
   */
  private String handlePushdownHeaders(final Path path, final Container theContainer,
                                       final List<GeneralHeader> pushdownHeaders) {
    String fullPath = path.toString();

    int queryStartIndex = fullPath.indexOf(PushdownStorletConstants.SWIFT_STORLET_QUERY_START);
    if (queryStartIndex < 0) {
      LOG.debug("No pushdown since " + PushdownStorletConstants.SWIFT_STORLET_QUERY_START
          + " not to be found in full path " + fullPath);
      return fullPath;
    }

    String queryPath = fullPath.substring(queryStartIndex + 1);

    String[] theParams =
      queryPath.split(PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_QUERY_SEPARATOR);  // ;
    int addedQueryParams = 0;

    for (String nextParam : theParams) {
      LOG.debug(" handlePushdownHeaders handling now " + nextParam);
      String[] paramParts = nextParam.split(
              PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_QUERY_PARAM_EQUAL);
      if (paramParts.length != 2) {
        LOG.warn("Skipping bad formed Storlet query parameter " + nextParam);
      } else {
        addStorletParameterHeader(pushdownHeaders,
                                  paramParts[0],
                                  paramTransform(paramParts[0], paramParts[1]));
        addedQueryParams++;
      }
    }

    if (addedQueryParams > 0) {
      // add all the headers needed for the storlet invocation:

      // 1. this is the header that request the CSV storlet invocation:
      pushdownHeaders.add(new GeneralHeader(
          PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_HEADER_NAME,
          PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_NAME)
      );

      LOG.debug("Number of added ##Headers to container is " + pushdownHeaders.size());
    }

    return fullPath.substring(0, queryStartIndex);
  }

  private String paramTransform(String paramKey, String paramVal) {
    if (PushdownStorletConstants.PUSHDOWN_COLUMNS.equals(paramKey)) {
      return paramVal.replace(":", "_");
    } else {
      return paramVal;
    }
  }

  private boolean isExpectedPushdownStorletParameter(String theKey) {
    // for (String nextKey : PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_QUERY_STRINGS) {
    //   if (nextKey.equals(theKey)) {
    //     return true;
    //   }
    // }
    // return false;
    return true;
  }

  private void addStorletParameterHeader(final List<GeneralHeader> pushdownHeaders,
                         final String key, final String val) {

    String paramKey = PushdownStorletConstants.SWIFT_PUSHDOWN_STORLET_PARAM_PREFIX
        + pushdownHeaders.size();
    String paramValue = key + PushdownStorletConstants.SWIFT_STORLET_QUERY_PARAM_EQUAL + val;
    LOG.debug(" ### addStorletParameterHeader  key = " + key + " val = " + val);
    pushdownHeaders.add(new GeneralHeader(paramKey, paramValue));
  }

}
