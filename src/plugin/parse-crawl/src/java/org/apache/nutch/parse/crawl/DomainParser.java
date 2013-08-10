package org.apache.nutch.parse.crawl;

import org.apache.hadoop.conf.Configuration;
import org.apache.nutch.parse.Outlink;

public interface DomainParser {

    public void parse(String paramString)
            throws Exception;

    public String getTitle();

    public String getCompanyName();

    public String getCompanyOverview();

    public String getCompanyAddress();

    public String getCompanyRange();

    public String getJobCategory();

    public String getJobLocation();

    public String getJobTimeWork();

    public String getJobMemberLevel();

    public String getJobSalary();

    public String getJobAge();

    public String getJobSex();

    public String getJobOverview();

    public String getJobEducationLevel();

    public String getJobExperienceLevel();

    public String getJobRequirement();

    public String getJobLanguage();

    public String getJobContactDetail();

    public String getJobContactName();

    public String getJobContactAddress();

    public String getJobContactPerson();

    public String getJobContactEmail();

    public String getJobContactPhone();

    public String getJobExpired();

    public Outlink[] getOutlinks();

    public void setConf(Configuration paramConfiguration);

    public Configuration getConf();
}