package org.apache.nutch.parse.crawl;

import org.xeustechnologies.jcl.JarClassLoader;
import org.xeustechnologies.jcl.JclObjectFactory;
import org.xeustechnologies.jcl.proxy.CglibProxyProvider;
import org.xeustechnologies.jcl.proxy.ProxyProviderFactory;

/**
 *
 * @author Tran Anh tuan <tuanta2@peacesoft.net>
 */
public class DomainClassLoader {

    JclObjectFactory factory = null;
    JarClassLoader jcl = null;

    public DomainClassLoader() {
        jcl = new JarClassLoader();
        jcl.add("crawl-plugins/");
        ProxyProviderFactory.setDefaultProxyProvider(new CglibProxyProvider());
        //Create a factory of castable objects/proxies
        factory = JclObjectFactory.getInstance(true);
    }

    public DomainParser getClass(String className) throws Exception {
        return (DomainParser) factory.create(jcl, className);
    }
}
