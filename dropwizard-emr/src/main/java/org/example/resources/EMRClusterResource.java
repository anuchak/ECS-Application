package org.example.resources;

import com.wordnik.swagger.annotations.ApiParam;
import io.swagger.annotations.Api;
import io.swagger.annotations.ApiOperation;
import org.example.api.NewClusterResponse;
import org.example.api.SubmitJobResponse;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;
import java.util.List;

@Path("/v1/emr")
@Api(value = "/v1/emr", description = "EMR related API")
@Produces(MediaType.APPLICATION_JSON)
public class EMRClusterResource
{
    private final EMRClusterMain emrClusterMain;

    public EMRClusterResource()
    {
        this.emrClusterMain = new EMRClusterMain();
    }

    @POST
    @Path("/createCluster")
    @ApiOperation(httpMethod = "POST", value= "create cluster")
    public NewClusterResponse createEmrCluster(
        @ApiParam(value = "Cluster Logs S3 Bucket", required = true) @FormParam("s3BucketLogsLocation") String s3BucketLogsLocation,
        @ApiParam(value = "EMR Cluster Instance Type", required = true) @FormParam("emrClusterInstanceType") String emrClusterInstanceType,
        @ApiParam(value = "No. of EMR Instances", required = true) @FormParam("emrClusterInstanceCount") int emrClusterInstanceCount
        )
    {
        return this.emrClusterMain.startCluster(s3BucketLogsLocation, emrClusterInstanceType, emrClusterInstanceCount);
    }

    @GET
    @Path("/getAllClusters")
    @ApiOperation(httpMethod = "GET", value= "get all clusters")
    public List<String> getAllClusters()
    {
      return this.emrClusterMain.getAllClusterList();
    }

    @POST
    @Path("/submitJob")
    @ApiOperation(httpMethod = "POST", value= "submit job to cluster")
    public SubmitJobResponse submitJobToCluster(
            @ApiParam(value = "Cluster ID", required = true) @FormParam("clusterId") String clusterId,
            @ApiParam(value = "AWS S3 location of JAR / PyWheel", required = true) @FormParam("s3PackageLocation") String s3PackageLocation,
            @ApiParam(value = "Main Entry Point of Logic", required = true) @FormParam("mainEntryPoint") String mainEntryPoint,
            @ApiParam(value = "Spark Executor Instance count", required = true) @FormParam("sparkExecutorInstances") String sparkExecutorInstances,
            @ApiParam(value = "Spark Executor Instance Core count", required = true) @FormParam("sparkExecutorCores") String sparkExecutorCores,
            @ApiParam(value = "Spark Executor Instance Memory", required = true) @FormParam("sparkExecutorMemory") String sparkExecutorMemory,
            @ApiParam(value = "Spark Driver Instance count", required = true) @FormParam("sparkDriverMemory") String sparkDriverMemory,
            @ApiParam(value = "Python (p) / Java (j) calculation", required = true) @FormParam("pythonOrJava") String pythonOrJava
            )
    {
        return this.emrClusterMain.runJob(clusterId, s3PackageLocation, mainEntryPoint, sparkExecutorInstances, sparkExecutorCores, sparkExecutorMemory,
            sparkDriverMemory, pythonOrJava);
    }

    @DELETE
    @Path("/deleteClusters")
    @ApiOperation(httpMethod = "DELETE", value= "delete all clusters")
    public boolean deleteEmrClusters()
    {
        try
        {
            this.emrClusterMain.terminateClusters();
            return true;
        }
        catch(Exception e)
        {
            throw e;
        }
    }

}
