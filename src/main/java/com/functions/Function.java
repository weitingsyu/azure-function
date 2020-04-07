/**
 * Copyright (c) Microsoft Corporation. All rights reserved.
 * Licensed under the MIT License. See License.txt in the project root for
 * license information.
 */

package com.functions;

import com.microsoft.azure.functions.ExecutionContext;
import com.microsoft.azure.functions.annotation.BindingName;
import com.microsoft.azure.functions.annotation.BlobTrigger;
import com.microsoft.azure.functions.annotation.FunctionName;

/**
 * Azure Functions with HTTP Trigger.
 */
public class Function {

    private final static String ecrDev = "eventhub/wistronssoteventhub/whq.plm.ccm.ecr/{name}";    

    @FunctionName("EcrAvroProcessor")
    public void ecrAvroProcessor(
            @BlobTrigger(name = "EcrAvroTigger", dataType = "binary", path = ecrDev, connection = "AzureWebJobsStorage") byte[] content,
            @BindingName("name") String filename, final ExecutionContext context) {
        context.getLogger().info("Name: " + filename + " Size: " + content.length + "bytes");
        EcrAvroToCsv.processAvroFile(content);
        context.getLogger().info("Finish execute ECR - Name: " + filename);

    }

}
