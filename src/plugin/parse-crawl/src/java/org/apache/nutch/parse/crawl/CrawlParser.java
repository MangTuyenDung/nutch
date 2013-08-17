/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to You under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */
package org.apache.nutch.parse.crawl;

import domain.DomainParser;
import java.util.Map;
import java.net.URL;
import java.net.MalformedURLException;
import java.nio.charset.Charset;
import java.io.*;
import java.util.Arrays;
import java.util.logging.Level;
import java.util.regex.*;
import org.apache.commons.codec.binary.Base64;
import org.apache.commons.httpclient.HttpClient;
import org.apache.commons.httpclient.methods.PostMethod;

import org.cyberneko.html.parsers.*;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.w3c.dom.*;
import org.apache.html.dom.*;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.nutch.metadata.Metadata;
import org.apache.nutch.metadata.Nutch;
import org.apache.nutch.protocol.Content;
import org.apache.hadoop.conf.*;
import org.apache.nutch.parse.*;
import org.apache.nutch.util.*;

public class CrawlParser implements Parser {

    public static final Logger LOG = LoggerFactory.getLogger("org.apache.nutch.parse.crawl");
    // I used 1000 bytes at first, but  found that some documents have 
    // meta tag well past the first 1000 bytes. 
    // (e.g. http://cn.promo.yahoo.com/customcare/music.html)
    private static final int CHUNK_SIZE = 2000;
    // NUTCH-1006 Meta equiv with single quotes not accepted
    private static Pattern metaPattern =
            Pattern.compile("<meta\\s+([^>]*http-equiv=(\"|')?content-type(\"|')?[^>]*)>",
            Pattern.CASE_INSENSITIVE);
    private static Pattern charsetPattern =
            Pattern.compile("charset=\\s*([a-z][_\\-0-9a-z]*)",
            Pattern.CASE_INSENSITIVE);
    private String parserImpl;

    /**
     * Given a
     * <code>byte[]</code> representing an html file of an
     * <em>unknown</em> encoding, read out 'charset' parameter in the meta tag
     * from the first
     * <code>CHUNK_SIZE</code> bytes. If there's no meta tag for Content-Type or
     * no charset is specified,
     * <code>null</code> is returned. <br />
     * FIXME: non-byte oriented character encodings (UTF-16, UTF-32) can't be
     * handled with this. We need to do something similar to what's done by
     * mozilla
     * (http://lxr.mozilla.org/seamonkey/source/parser/htmlparser/src/nsParser.cpp#1993).
     * See also http://www.w3.org/TR/REC-xml/#sec-guessing
     * <br />
     *
     * @param content <code>byte[]</code> representation of an html file
     */
    private static String sniffCharacterEncoding(byte[] content) {
        int length = content.length < CHUNK_SIZE
                ? content.length : CHUNK_SIZE;

        // We don't care about non-ASCII parts so that it's sufficient
        // to just inflate each byte to a 16-bit value by padding. 
        // For instance, the sequence {0x41, 0x82, 0xb7} will be turned into 
        // {U+0041, U+0082, U+00B7}. 
        String str = "";
        try {
            str = new String(content, 0, length,
                    Charset.forName("ASCII").toString());
        } catch (UnsupportedEncodingException e) {
            // code should never come here, but just in case... 
            return null;
        }

        Matcher metaMatcher = metaPattern.matcher(str);
        String encoding = null;
        if (metaMatcher.find()) {
            Matcher charsetMatcher = charsetPattern.matcher(metaMatcher.group(1));
            if (charsetMatcher.find()) {
                encoding = charsetMatcher.group(1);
            }
        }

        return encoding;
    }
    private String defaultCharEncoding;
    private Configuration conf;
    private HtmlParseFilters htmlParseFilters;
    private String cachingPolicy;

    public ParseResult getParse(Content content) {
        HTMLMetaTags metaTags = new HTMLMetaTags();

        URL base;
        try {
            base = new URL(content.getBaseUrl());
        } catch (MalformedURLException e) {
            return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
        }

        String text = "";
        String title = "";
        Outlink[] outlinks = new Outlink[0];
        Metadata metadata = new Metadata();

        //Start parse domain name
        String domain = URLUtil.getDomainName(base).replaceAll("[^a-zA-Z0-9]+", "");
        DomainClassLoader classLoader = new DomainClassLoader();

        DomainParser domainParser;
        try {
            if (!domain.startsWith("www")) {
                domain = "www" + domain;
            }
            domainParser = classLoader.getClass("domain." + domain.toUpperCase());
            domainParser.setConf(getConf());
            domainParser.parse(content.getBaseUrl());

            title = domainParser.getTitle();
            text = domainParser.getJobOverview().replaceAll("<!--.*?-->", "").replaceAll("<[^>]+>", "");
            outlinks = domainParser.getOutlinks();
            if ((text.isEmpty())
                    && (LOG.isWarnEnabled())) {
                LOG.warn(new StringBuilder().append("CrawlParser: Empty content from ").append(content.getBaseUrl()).toString());
            }

	    if (!domainParser.getJobOverview().isEmpty()) {
                metadata.set("jobOverview", domainParser.getJobOverview()); 

            if (!domainParser.getCompanyName().isEmpty()) {
                metadata.set("companyName", domainParser.getCompanyName());
            }

            if (!domainParser.getCompanyOverview().isEmpty()) {
                metadata.set("companyOverview", domainParser.getCompanyOverview());
            }

            if (!domainParser.getCompanyAddress().isEmpty()) {
                metadata.set("companyAddress", domainParser.getCompanyAddress());
            }

            if (!domainParser.getCompanyRange().isEmpty()) {
                metadata.set("companyRange", domainParser.getCompanyRange());
            }

            if (!domainParser.getJobCategory().isEmpty()) {
                metadata.set("jobCategory", domainParser.getJobCategory());
            }

            if (!domainParser.getJobLocation().isEmpty()) {
                metadata.set("jobLocation", domainParser.getJobLocation());
            }

            if (!domainParser.getJobTimeWork().isEmpty()) {
                metadata.set("jobTimeWork", domainParser.getJobTimeWork());
            }

            if (!domainParser.getJobMemberLevel().isEmpty()) {
                metadata.set("jobMemberLevel", domainParser.getJobMemberLevel());
            }

            if (!domainParser.getJobSalary().isEmpty()) {
                metadata.set("jobSalary", domainParser.getJobSalary());
            }

            if (!domainParser.getJobAge().isEmpty()) {
                metadata.set("jobAge", domainParser.getJobAge());
            }

            if (!domainParser.getJobSex().isEmpty()) {
                metadata.set("jobSex", domainParser.getJobSex());
            }

            if (!domainParser.getJobEducationLevel().isEmpty()) {
                metadata.set("jobEducationLevel", domainParser.getJobEducationLevel());
            }

            if (!domainParser.getJobExperienceLevel().isEmpty()) {
                metadata.set("jobExperienceLevel", domainParser.getJobExperienceLevel());
            }

            if (!domainParser.getJobRequirement().isEmpty()) {
                metadata.set("jobRequirement", domainParser.getJobRequirement());
            }

            if (!domainParser.getJobLanguage().isEmpty()) {
                metadata.set("jobLanguage", domainParser.getJobLanguage());
            }

            if (!domainParser.getJobContactDetail().isEmpty()) {
                metadata.set("jobContactDetail", domainParser.getJobContactDetail());
            }

            if (!domainParser.getJobContactName().isEmpty()) {
                metadata.set("jobContactName", domainParser.getJobContactName());
            }

            if (!domainParser.getJobContactAddress().isEmpty()) {
                metadata.set("jobContactAddress", domainParser.getJobContactAddress());
            }

            if (!domainParser.getJobContactPerson().isEmpty()) {
                metadata.set("jobContactPerson", domainParser.getJobContactPerson());
            }

            if (!domainParser.getJobContactEmail().isEmpty()) {
                metadata.set("jobContactEmail", domainParser.getJobContactEmail());
            }

            if (!domainParser.getJobContactPhone().isEmpty()) {
                metadata.set("jobContactPhone", domainParser.getJobContactPhone());
            }

            if (domainParser.getJobExpired() != null) {
                metadata.set("jobExpired", domainParser.getJobExpired());
            }

            if (LOG.isDebugEnabled()) {
                LOG.debug(new StringBuilder().append("CrawlParser: content crawl value from  ").append(content.getBaseUrl()).toString());
                for (String name : metadata.names()) {
                    LOG.debug(new StringBuilder().append("[").append(name).append(":").append(metadata.get(name).length() > 20 ? metadata.get(name).substring(0, 20) : metadata.get(name)).append("]").toString());
                }
            }
            
            LOG.info("CrawlParser title " + title);
            LOG.info("CrawlParser category " + domainParser.getJobCategory());
            
            HttpClient client = new HttpClient();
            PostMethod method = new PostMethod(parseHtmlLink);

            addPostParameter(method, System.currentTimeMillis() + "", "id", false);
            addPostParameter(method, domainParser.getTitle(), "title", true);
            addPostParameter(method, base.toString(), "url", false);
            addPostParameter(method, "1.0", "boost", false);
            addPostParameter(method, URLUtil.getDomainName(base), "domain", false);
            if (text.length() > 160) {
                addPostParameter(method, text.substring(0, 160), "content", true);
            } else {
                addPostParameter(method, text, "content", true);
            }

            addPostParameter(method, domainParser.getCompanyName(), "companyName", true);
            addPostParameter(method, domainParser.getCompanyOverview(), "companyOverview", true);
            addPostParameter(method, domainParser.getCompanyAddress(), "companyAddress", true);
            addPostParameter(method, domainParser.getCompanyRange(), "companyRange", true);

            addPostParameter(method, domainParser.getJobCategory(), "jobCategory", true);
            addPostParameter(method, domainParser.getJobLocation(), "jobLocation", true);
            addPostParameter(method, domainParser.getJobTimeWork(), "jobTimeWork", true);
            addPostParameter(method, domainParser.getJobMemberLevel(), "jobMemberLevel", true);
            addPostParameter(method, domainParser.getJobSalary(), "jobSalary", true);
            addPostParameter(method, domainParser.getJobAge(), "jobAge", true);
            addPostParameter(method, domainParser.getJobSex(), "jobSex", true);
            addPostParameter(method, domainParser.getJobOverview(), "jobOverview", true);

            addPostParameter(method, domainParser.getJobEducationLevel(), "jobEducationLevel", true);
            addPostParameter(method, domainParser.getJobExperienceLevel(), "jobExperienceLevel", true);
            addPostParameter(method, domainParser.getJobRequirement(), "jobRequirement", true);

            addPostParameter(method, domainParser.getJobLanguage(), "jobLanguage", true);
            addPostParameter(method, domainParser.getJobContactDetail(), "jobContactDetail", true);
            addPostParameter(method, domainParser.getJobContactName(), "jobContactName", true);
            addPostParameter(method, domainParser.getJobContactAddress(), "jobContactAddress", true);
            addPostParameter(method, domainParser.getJobContactPerson(), "jobContactPerson", true);
            addPostParameter(method, domainParser.getJobContactPhone(), "jobContactPhone", true);
            addPostParameter(method, domainParser.getJobContactEmail(), "jobContactEmail", false);

            addPostParameter(method, domainParser.getJobExpired(), "jobExpired", false);

            int returnCode = client.executeMethod(method);
            byte[] data = method.getResponseBody();
            if (returnCode != 200) {
                toFile("logs-post/" + System.currentTimeMillis() + "-error.html", data);
                toFile("logs-post/" + System.currentTimeMillis() + "-post-error.html", Arrays.toString(method.getParameters()).getBytes());
            }
            LOG.info("Status " + returnCode + " post data " + base);
	    }
        } catch (Exception ex) {
            LOG.error("CrawlParser: ", ex);
            return new ParseStatus(ex).getEmptyParseResult(content.getUrl(), getConf());
        }
        // parse the content
        DocumentFragment root;
        try {
            byte[] contentInOctets = content.getContent();
            InputSource input = new InputSource(new ByteArrayInputStream(contentInOctets));

            EncodingDetector detector = new EncodingDetector(conf);
            detector.autoDetectClues(content, true);
            detector.addClue(sniffCharacterEncoding(contentInOctets), "sniffed");
            String encoding = detector.guessEncoding(content, defaultCharEncoding);

            metadata.set(Metadata.ORIGINAL_CHAR_ENCODING, encoding);
            metadata.set(Metadata.CHAR_ENCODING_FOR_CONVERSION, encoding);

            input.setEncoding(encoding);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Parsing...");
            }
            root = parse(input);
        } catch (IOException e) {
            return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
        } catch (DOMException e) {
            return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
        } catch (SAXException e) {
            return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
        } catch (Exception e) {
            LOG.error("Error: ", e);
            return new ParseStatus(e).getEmptyParseResult(content.getUrl(), getConf());
        }

        ParseStatus status = new ParseStatus(ParseStatus.SUCCESS);
        if (metaTags.getRefresh()) {
            status.setMinorCode(ParseStatus.SUCCESS_REDIRECT);
            status.setArgs(new String[]{metaTags.getRefreshHref().toString(),
                Integer.toString(metaTags.getRefreshTime())});
        }
        ParseData parseData = new ParseData(status, title, outlinks,
                content.getMetadata(), metadata);
        ParseResult parseResult = ParseResult.createParseResult(content.getUrl(),
                new ParseImpl(text, parseData));

        // run filters on parse
        ParseResult filteredParse = this.htmlParseFilters.filter(content, parseResult,
                metaTags, root);
        if (metaTags.getNoCache()) {             // not okay to cache
            for (Map.Entry<org.apache.hadoop.io.Text, Parse> entry : filteredParse) {
                entry.getValue().getData().getParseMeta().set(Nutch.CACHING_FORBIDDEN_KEY,
                        cachingPolicy);
            }
        }
        return filteredParse;
    }

    private void addPostParameter(PostMethod method, String value, String key, boolean encode) {
        if (value != null) {
            if (encode) {
                method.addParameter(key, Base64.encodeBase64String(value.trim().getBytes()));
            } else {
                method.addParameter(key, value.trim());
            }
        }
    }

    public static void toFile(String fileName, byte[] data) {
        FileOutputStream fos = null;
        try {
            File f = new File(fileName);
            fos = new FileOutputStream(f);
            fos.write(data);
            fos.flush();
            fos.close();
        } catch (FileNotFoundException ex) {
        } catch (Exception ex) {
            LOG.info("Loi khi xuat ra file: " + fileName);
        } finally {
            try {
                if (fos != null) {
                    fos.close();
                }
            } catch (Exception ex) {
            }
        }
    }

    private DocumentFragment parse(InputSource input) throws Exception {
        if (parserImpl.equalsIgnoreCase("tagsoup")) {
            return parseTagSoup(input);
        } else {
            return parseNeko(input);
        }
    }

    private DocumentFragment parseTagSoup(InputSource input) throws Exception {
        HTMLDocumentImpl doc = new HTMLDocumentImpl();
        DocumentFragment frag = doc.createDocumentFragment();
        DOMBuilder builder = new DOMBuilder(doc, frag);
        org.ccil.cowan.tagsoup.Parser reader = new org.ccil.cowan.tagsoup.Parser();
        reader.setContentHandler(builder);
        reader.setFeature(org.ccil.cowan.tagsoup.Parser.ignoreBogonsFeature, true);
        reader.setFeature(org.ccil.cowan.tagsoup.Parser.bogonsEmptyFeature, false);
        reader.setProperty("http://xml.org/sax/properties/lexical-handler", builder);
        reader.parse(input);
        return frag;
    }

    private DocumentFragment parseNeko(InputSource input) throws Exception {
        DOMFragmentParser parser = new DOMFragmentParser();
        try {
            parser.setFeature("http://cyberneko.org/html/features/augmentations",
                    true);
            parser.setProperty("http://cyberneko.org/html/properties/default-encoding",
                    defaultCharEncoding);
            parser.setFeature("http://cyberneko.org/html/features/scanner/ignore-specified-charset",
                    true);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/ignore-outside-content",
                    false);
            parser.setFeature("http://cyberneko.org/html/features/balance-tags/document-fragment",
                    true);
            parser.setFeature("http://cyberneko.org/html/features/report-errors",
                    LOG.isTraceEnabled());
        } catch (SAXException e) {
        }
        // convert Document to DocumentFragment
        HTMLDocumentImpl doc = new HTMLDocumentImpl();
        doc.setErrorChecking(false);
        DocumentFragment res = doc.createDocumentFragment();
        DocumentFragment frag = doc.createDocumentFragment();
        parser.parse(input, frag);
        res.appendChild(frag);

        try {
            while (true) {
                frag = doc.createDocumentFragment();
                parser.parse(input, frag);
                if (!frag.hasChildNodes()) {
                    break;
                }
                if (LOG.isInfoEnabled()) {
                    LOG.info(" - new frag, " + frag.getChildNodes().getLength() + " nodes.");
                }
                res.appendChild(frag);
            }
        } catch (Exception e) {
            LOG.error("Error: ", e);
        }
        return res;
    }

    public static void main(String[] args) throws Exception {
        //LOG.setLevel(Level.FINE);
        String name = args[0];
        String url = "file:" + name;
        File file = new File(name);
        byte[] bytes = new byte[(int) file.length()];
        DataInputStream in = new DataInputStream(new FileInputStream(file));
        in.readFully(bytes);
        Configuration conf = NutchConfiguration.create();
        CrawlParser parser = new CrawlParser();
        parser.setConf(conf);
        Parse parse = parser.getParse(
                new Content(url, url, bytes, "text/html", new Metadata(), conf)).get(url);
        System.out.println("data: " + parse.getData());

        System.out.println("text: " + parse.getText());

    }

    public void setConf(Configuration conf) {
        this.conf = conf;
        this.htmlParseFilters = new HtmlParseFilters(getConf());
        this.parserImpl = getConf().get("parser.html.impl", "neko");
        this.parseHtmlLink = getConf().get("parser.html.link", "http://mangtuyendung.vn/crawl");
        this.defaultCharEncoding = getConf().get(
                "parser.character.encoding.default", "windows-1252");
        this.cachingPolicy = getConf().get("parser.caching.forbidden.policy",
                Nutch.CACHING_FORBIDDEN_CONTENT);
    }
    private String parseHtmlLink;

    public Configuration getConf() {
        return this.conf;
    }
}
