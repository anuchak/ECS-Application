package org.example.api;

import com.amazonaws.services.elasticmapreduce.model.AddJobFlowStepsResult;
import com.fasterxml.jackson.annotation.JsonProperty;

public class SubmitJobResponse
{
    @JsonProperty
    private final AddJobFlowStepsResult addJobFlowStepsResult;

    public SubmitJobResponse(AddJobFlowStepsResult addJobFlowStepsResult) {
        this.addJobFlowStepsResult = addJobFlowStepsResult;
    }

    public AddJobFlowStepsResult getAddJobFlowStepsResult() {
        return addJobFlowStepsResult;
    }
}
