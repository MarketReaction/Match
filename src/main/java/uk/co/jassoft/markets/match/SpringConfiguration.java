package uk.co.jassoft.markets.match;

import uk.co.jassoft.markets.BaseSpringConfiguration;
import org.springframework.boot.SpringApplication;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.Configuration;

/**
 * Created by jonshaw on 13/07/15.
 */
@Configuration
@ComponentScan("uk.co.jassoft.markets.match")
public class SpringConfiguration extends BaseSpringConfiguration {

    public static void main(String[] args) throws Exception {
        SpringApplication.run(SpringConfiguration.class, args);
    }
}
