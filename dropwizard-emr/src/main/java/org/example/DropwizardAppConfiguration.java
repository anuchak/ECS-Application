package org.example;

import io.dropwizard.Configuration;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;

public class DropwizardAppConfiguration extends Configuration {

    @JsonProperty("swagger")
    private final SwaggerBundleConfiguration swaggerBundleConfiguration = new SwaggerBundleConfiguration();

    public SwaggerBundleConfiguration getSwaggerBundleConfiguration() {
        return swaggerBundleConfiguration;
    }
}
