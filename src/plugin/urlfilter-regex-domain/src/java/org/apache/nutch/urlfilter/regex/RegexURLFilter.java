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
package org.apache.nutch.urlfilter.regex;

// JDK imports
import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.net.MalformedURLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Properties;
import java.util.regex.Pattern;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.nutch.net.URLFilter;
import org.apache.nutch.util.URLUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filters URLs based on a file of regular expressions using the
 * {@link java.util.regex Java Regex implementation}.
 */
public class RegexURLFilter implements URLFilter {

    /**
     * My logger
     */
    private final static Logger LOG = LoggerFactory.getLogger(RegexURLFilter.class);
    private FileSystem fs;
    /**
     * An array of domain rules
     */
    private LinkedHashMap<String, List<RegexRule>> domainRules = new LinkedHashMap<String, List<RegexRule>>();
    /**
     * The current configuration
     */
    private Configuration conf;

    /**
     * Rules specified as a config property will override rules specified as a
     * config file.
     */
    protected Reader getRulesReader(String domain) throws IOException {
        fs = FileSystem.get(getConf());
        Path rootpath = new Path(getConf().get("urlfilter.domain.path", "."));
        Path file = new Path(rootpath, "crawl-plugins/" + domain.toLowerCase() + "-regex.txt");
        return new InputStreamReader(fs.open(file));
    }

    public String filter(String url) {

        String domain = "";
        try {
            domain = URLUtil.getDomainName(url);
        } catch (MalformedURLException ex) {
            return null;
        }
        List<RegexRule> dRulus = domainRules.get(domain);
        if (dRulus == null || (dRulus != null && dRulus.isEmpty())) {
            Reader reader = null;
            try {
                reader = getRulesReader(domain);
            } catch (Exception e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage());
                }
                throw new RuntimeException(e.getMessage(), e);
            }
            try {
                dRulus = readRules(reader);
            } catch (IOException e) {
                if (LOG.isErrorEnabled()) {
                    LOG.error(e.getMessage());
                }
                throw new RuntimeException(e.getMessage(), e);
            }
            if (dRulus != null && !dRulus.isEmpty()) {
                domainRules.put(domain, dRulus);
            }
        }

        for (RegexRule rule : dRulus) {
            if (rule.match(url)) {
                return rule.accept() ? url : null;
            }
        }
        return null;
    }

    /**
     * Read the specified file of rules.
     *
     * @param reader is a reader of regular expressions rules.
     * @return the corresponding {
     * @RegexRule rules}.
     */
    private List<RegexRule> readRules(Reader reader)
            throws IOException, IllegalArgumentException {

        BufferedReader in = new BufferedReader(reader);
        List<RegexRule> rules = new ArrayList<RegexRule>();
        String line;

        while ((line = in.readLine()) != null) {
            if (line.length() == 0) {
                continue;
            }
            char first = line.charAt(0);
            boolean sign = false;
            switch (first) {
                case '+':
                    sign = true;
                    break;
                case '-':
                    sign = false;
                    break;
                case ' ':
                case '\n':
                case '#':           // skip blank & comment lines
                    continue;
                default:
                    throw new IOException("Invalid first character: " + line);
            }

            String regex = line.substring(1);
            if (LOG.isTraceEnabled()) {
                LOG.trace("Adding rule [" + regex + "]");
            }
            RegexRule rule = createRule(sign, regex);
            rules.add(rule);
        }
        return rules;
    }

    // Inherited Javadoc
    protected RegexRule createRule(boolean sign, String regex) {
        return new Rule(sign, regex);
    }

    /* ------------------------------------ *
     * </implementation:RegexURLFilterBase> *
     * ------------------------------------ */
    public static void main(String args[]) throws IOException {
//        RegexURLFilter filter = new RegexURLFilter();
    }

    public void setConf(Configuration c) {
        this.conf = c;
    }

    public Configuration getConf() {
        return conf;
    }

    private class Rule extends RegexRule {

        private Pattern pattern;

        Rule(boolean sign, String regex) {
            super(sign, regex);
            pattern = Pattern.compile(regex);
        }

        protected boolean match(String url) {
            return pattern.matcher(url).find();
        }
    }
}
