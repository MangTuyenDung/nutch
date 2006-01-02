/**
 * Copyright 2005 The Apache Software Foundation
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
 */

package org.apache.nutch.parse.zip;

// JDK imports
import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Properties;
import java.util.logging.Logger;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.net.URL;

// Nutch imports
import org.apache.nutch.parse.Parse;
import org.apache.nutch.parse.ParseUtil;
import org.apache.nutch.parse.ParseData;
import org.apache.nutch.parse.ParseException;
import org.apache.nutch.parse.Outlink;
import org.apache.nutch.protocol.Content;
import org.apache.nutch.protocol.ContentProperties;
import org.apache.nutch.util.LogFormatter;
import org.apache.nutch.util.NutchConf;
import org.apache.nutch.util.mime.MimeTypes;


/**
 *
 * @author Rohit Kulkarni & Ashish Vaidya
 */
public class ZipTextExtractor {
  
  /** Get the MimeTypes resolver instance. */
  private final static MimeTypes MIME =
          MimeTypes.get(NutchConf.get().get("mime.types.file"));
  
  public static final Logger LOG = LogFormatter.getLogger(ZipTextExtractor.class.getName());
  
  
  /** Creates a new instance of ZipTextExtractor */
  public ZipTextExtractor() {
  }
  
  public String extractText(InputStream input, String url, List outLinksList) throws IOException {
    String resultText = "";
    byte temp;
    
    ZipInputStream zin = new ZipInputStream(input);
    
    ZipEntry entry;
    
    while ((entry = zin.getNextEntry()) != null) {
      
      if (!entry.isDirectory()) {
        int size = (int) entry.getSize();
        byte[] b = new byte[size];
        for(int x = 0; x < size; x++) {
          int err = zin.read();
          if(err != -1) {
            b[x] = (byte)err;
          }
        }
        String newurl = url + "/";
        String fname = entry.getName();
        newurl += fname;
        URL aURL = new URL(newurl);
        String base = aURL.toString();
        int i = fname.lastIndexOf('.');
        if (i != -1) {
          // Trying to resolve the Mime-Type
          String contentType = MIME.getMimeType(fname).getName();
          try {
            ContentProperties metadata = new ContentProperties();
            metadata.setProperty("Content-Length", Long.toString(entry.getSize()));
            metadata.setProperty("Content-Type", contentType);
            Content content = new Content(newurl, base, b, contentType, metadata);
            Parse parse = ParseUtil.parse(content);
            ParseData theParseData = parse.getData();
            Outlink[] theOutlinks = theParseData.getOutlinks();
            
            for(int count = 0; count < theOutlinks.length; count++) {
              outLinksList.add(new Outlink(theOutlinks[count].getToUrl(), theOutlinks[count].getAnchor()));
            }
            
            resultText += entry.getName() + " " + parse.getText() + " ";
          } catch (ParseException e) {
            
            LOG.info("fetch okay, but can't parse " + fname + ", reason: " + e.getMessage());
          }
        }
      }
    }
    
    return resultText;
  }
  
}
