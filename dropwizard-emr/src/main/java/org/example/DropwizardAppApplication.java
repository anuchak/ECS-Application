package org.example;

import io.dropwizard.Application;
import io.dropwizard.setup.Bootstrap;
import io.dropwizard.setup.Environment;
import io.federecio.dropwizard.swagger.SwaggerBundle;
import io.federecio.dropwizard.swagger.SwaggerBundleConfiguration;
import org.example.resources.EMRClusterMain;
import org.example.resources.EMRClusterResource;


public class DropwizardAppApplication extends Application<DropwizardAppConfiguration> {

    public static void main(final String[] args) throws Exception {
        new DropwizardAppApplication().run(args);
    }

    @Override
    public String getName() {
        return "DropwizardApp";
    }

    @Override
    public void initialize(final Bootstrap<DropwizardAppConfiguration> bootstrap) {
        bootstrap.addBundle(new SwaggerBundle<DropwizardAppConfiguration>()
        {
            @Override
            protected SwaggerBundleConfiguration getSwaggerBundleConfiguration(DropwizardAppConfiguration dropwizardAppConfiguration) {
                return dropwizardAppConfiguration.getSwaggerBundleConfiguration();
            }

        });
    }

    @Override
    public void run(final DropwizardAppConfiguration configuration,
                    final Environment environment) {
        environment.jersey().register(new EMRClusterResource());
    }

}
