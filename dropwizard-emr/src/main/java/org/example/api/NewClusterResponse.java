package org.example.api;

import com.amazonaws.services.elasticmapreduce.model.RunJobFlowResult;
import com.fasterxml.jackson.annotation.JsonProperty;

public class NewClusterResponse
{
    @JsonProperty
    private final String emrClusterID;

    @JsonProperty
    private final RunJobFlowResult runJobFlowResult;

    public NewClusterResponse(String emrClusterID, RunJobFlowResult runJobFlowResult)
    {
        this.emrClusterID = emrClusterID;
        this.runJobFlowResult = runJobFlowResult;
    }


    public String getEmrClusterID() {
        return emrClusterID;
    }

    public RunJobFlowResult getRunJobFlowResult() {
        return runJobFlowResult;
    }
}
