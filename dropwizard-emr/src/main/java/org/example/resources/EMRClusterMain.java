package org.example.resources;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduce;
import com.amazonaws.services.elasticmapreduce.AmazonElasticMapReduceClientBuilder;
import com.amazonaws.services.elasticmapreduce.model.*;
import com.amazonaws.services.elasticmapreduce.util.StepFactory;
import org.example.api.NewClusterResponse;
import org.example.api.SubmitJobResponse;

import java.util.*;

public class EMRClusterMain
{
    private final AWSCredentialsProvider credentialsProvider;
    private final List<String> clusterList = new ArrayList<>();
    private final AmazonElasticMapReduce EMR_CLIENT;

    public EMRClusterMain()
    {
        credentialsProvider = new ProfileCredentialsProvider("default");
        EMR_CLIENT = AmazonElasticMapReduceClientBuilder.standard()
                .withCredentials(new AWSStaticCredentialsProvider(credentialsProvider.getCredentials()))
                .withRegion(Regions.US_EAST_2)
                .build();
    }

    public NewClusterResponse startCluster(String s3BucketClusterLogs, String emrClusterInstanceType, int emrClusterInstanceCount)
    {
        s3BucketClusterLogs = Objects.isNull(s3BucketClusterLogs) ? "s3://aws-emr-cluster-logs/main-cluster-log-folder/" : s3BucketClusterLogs;

        // this is for enabling debugging in AWS Mmg console
        StepFactory stepFactory = new StepFactory("us-east-2.elasticmapreduce");
        StepConfig enabledebugging = new StepConfig()
                .withName("enable debugging")
                .withActionOnFailure("TERMINATE_JOB_FLOW")
                .withHadoopJarStep(stepFactory.newEnableDebuggingStep());

        Application spark = new Application().withName("Spark");

        String accessKey = credentialsProvider.getCredentials().getAWSAccessKeyId();
        String secretKey = credentialsProvider.getCredentials().getAWSSecretKey();
        Map<String, String> properties = new HashMap<>();
        properties.put("fs.s3a.endpoint", "s3.amazonaws.com");
        properties.put("fs.s3a.access.key", accessKey);
        properties.put("fs.s3a.secret.key", secretKey);

        Configuration coreSite = new Configuration()
                .withClassification("core-site")
                .withProperties(properties);

        InstanceGroupConfig instanceGroupConfigMaster = new InstanceGroupConfig()
                .withInstanceCount(1).withInstanceRole(InstanceRoleType.MASTER).withInstanceType(emrClusterInstanceType)
                .withMarket(MarketType.ON_DEMAND);

        InstanceGroupConfig instanceGroupConfigCore = new InstanceGroupConfig()
                .withInstanceCount(emrClusterInstanceCount).withInstanceRole(InstanceRoleType.CORE).withInstanceType(emrClusterInstanceType)
                .withMarket(MarketType.ON_DEMAND);

        InstanceGroupConfig instanceGroupConfigTask = new InstanceGroupConfig()
                .withInstanceCount(emrClusterInstanceCount).withInstanceRole(InstanceRoleType.TASK).withInstanceType(emrClusterInstanceType)
                .withMarket(MarketType.ON_DEMAND);

        List<InstanceGroupConfig> igConfigList = new ArrayList<>();
        igConfigList.add(instanceGroupConfigMaster);
        igConfigList.add(instanceGroupConfigCore);
        igConfigList.add(instanceGroupConfigTask);

        ComputeLimits computeLimits = new ComputeLimits().withUnitType(ComputeLimitsUnitType.VCPU)
                .withMinimumCapacityUnits(30).withMaximumCapacityUnits(42).withMaximumCoreCapacityUnits(15);

        ManagedScalingPolicy managedScalingPolicy = new ManagedScalingPolicy()
                .withComputeLimits(computeLimits);

        JobFlowInstancesConfig config = new JobFlowInstancesConfig()
                .withInstanceGroups(igConfigList)
                .withEc2SubnetId("subnet-01ac117ebdf3e00be")
                .withEc2KeyName("myKeyPair")
                .withKeepJobFlowAliveWhenNoSteps(true);

        RunJobFlowRequest request = new RunJobFlowRequest()
                .withName("Spark Cluster Test")
                .withReleaseLabel("emr-5.33.0")
                .withSteps(enabledebugging)
                .withApplications(spark)
                .withConfigurations(coreSite)
                .withLogUri(s3BucketClusterLogs)
                .withServiceRole("EMR_DefaultRole")
                .withJobFlowRole("EMR_EC2_DefaultRole")
                .withVisibleToAllUsers(true)
                .withInstances(config)
                .withManagedScalingPolicy(managedScalingPolicy);

        RunJobFlowResult result = EMR_CLIENT.runJobFlow(request);
        clusterList.add(result.getJobFlowId());
        System.out.println("Result of create AWS EMR cluster is " + result + "\n");
        return new NewClusterResponse(result.getJobFlowId(), result);
    }

    public SubmitJobResponse runJob(String clusterId, String s3PackageLocation, String mainEntryPoint, String sparkExecutorInstances, String sparkExecutorCores, String sparkExecutorMemory,
                                    String sparkDriverMemory, String pythonOrJava)
    {
        System.out.println("Submitting job to AWS EMR cluster: " + clusterId);
        AddJobFlowStepsRequest req = new AddJobFlowStepsRequest();
        req.withJobFlowId(clusterId);
        List<StepConfig> stepConfigs = new ArrayList<>();
        HadoopJarStepConfig sparkStepConf;
        if(pythonOrJava.equals("p"))
            sparkStepConf = pythonSparkSubmitCommand(s3PackageLocation, mainEntryPoint, sparkDriverMemory, sparkExecutorMemory, sparkExecutorCores, sparkExecutorInstances);
        else
            sparkStepConf = javaSparkSubmitCommand(mainEntryPoint, s3PackageLocation, sparkDriverMemory, sparkExecutorMemory, sparkExecutorCores, sparkExecutorInstances);

        StepConfig sparkStep = new StepConfig()
                .withName("Spark Step")
                .withActionOnFailure("CONTINUE")
                .withHadoopJarStep(sparkStepConf);

        stepConfigs.add(sparkStep);
        req.withSteps(stepConfigs);
        AddJobFlowStepsResult result = EMR_CLIENT.addJobFlowSteps(req);
        System.out.println("Result of running job on cluster: " + result);
        return new SubmitJobResponse(result);
    }

    private HadoopJarStepConfig javaSparkSubmitCommand(String mainClass, String s3JarLocation, String sparkDriverMemory, String sparkExecutorMemory,
                                                       String sparkExecutorCores, String sparkExecutorInstances)
    {
        return new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
                        "--driver-memory", sparkDriverMemory, "--executor-memory", sparkExecutorMemory, "--executor-cores", sparkExecutorCores,
                        "--num-executors", sparkExecutorInstances, "--class", mainClass, s3JarLocation);
    }

    /*
    For Python applications, simply pass a .py file in the place of <application.jar> location,
    and add Python .whl or .py files to the search path with --py-files.
    */
    private HadoopJarStepConfig pythonSparkSubmitCommand(String s3PyWheelLocation, String pythonEntryPoint, String sparkDriverMemory, String sparkExecutorMemory,
                                                         String sparkExecutorCores, String sparkExecutorInstances)
    {
        return new HadoopJarStepConfig()
                .withJar("command-runner.jar")
                .withArgs("spark-submit", "--master", "yarn", "--deploy-mode", "cluster",
                        "--driver-memory", sparkDriverMemory, "--executor-memory", sparkExecutorMemory, "--executor-cores", sparkExecutorCores,
                        "--num-executors", sparkExecutorInstances, "--py-files", s3PyWheelLocation, pythonEntryPoint);
    }

    public void terminateClusters()
    {
        TerminateJobFlowsRequest request = new TerminateJobFlowsRequest()
                .withJobFlowIds(clusterList)
                .withRequestCredentialsProvider(credentialsProvider);

        TerminateJobFlowsResult result = EMR_CLIENT.terminateJobFlows(request);
        System.out.println("Result of cluster terminate operation: " + result + "\n");
    }

    public List <String> getAllClusterList()
    {
        return clusterList;
    }

}
