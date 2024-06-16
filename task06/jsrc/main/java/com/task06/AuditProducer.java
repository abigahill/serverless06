package com.task06;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.DynamodbEvent;
import com.amazonaws.services.lambda.runtime.events.models.dynamodb.AttributeValue;
import com.syndicate.deployment.annotations.environment.EnvironmentVariable;
import com.syndicate.deployment.annotations.environment.EnvironmentVariables;
import com.syndicate.deployment.annotations.events.DynamoDbTriggerEventSource;
import com.syndicate.deployment.annotations.lambda.LambdaHandler;
import com.syndicate.deployment.annotations.resources.DependsOn;
import com.syndicate.deployment.model.RetentionSetting;
import com.syndicate.deployment.model.ResourceType;
import com.syndicate.deployment.model.DeploymentRuntime;
import org.joda.time.DateTime;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@LambdaHandler(lambdaName = "audit_producer",
	roleName = "audit_producer-role",
	runtime = DeploymentRuntime.JAVA11,
	isPublishVersion = false,
	logsExpiration = RetentionSetting.SYNDICATE_ALIASES_SPECIFIED
)
@DependsOn(
	name = "Configuration",
	resourceType = ResourceType.DYNAMODB_TABLE
)
@DependsOn(
	name = "Audit",
	resourceType = ResourceType.DYNAMODB_TABLE
)
@DynamoDbTriggerEventSource(
		targetTable = "Configuration",
		batchSize = 1
)
@EnvironmentVariables(value = {
	@EnvironmentVariable(key = "region", value = "${region}"),
	@EnvironmentVariable(key = "target_table", value = "${target_table}")
})
public class AuditProducer implements RequestHandler<DynamodbEvent, Void> {
	private final AmazonDynamoDB dynamoDBClient = AmazonDynamoDBClientBuilder.standard()
			.withRegion(System.getenv("region")).build();
	private final DynamoDB dynamoDB = new DynamoDB(dynamoDBClient);

	public Void handleRequest(DynamodbEvent dynamodbEvent, Context context) {
		context.getLogger().log("dynamodbEvent: " + dynamodbEvent.toString());

		for (DynamodbEvent.DynamodbStreamRecord record : dynamodbEvent.getRecords()) {
			switch (record.getEventName()) {
				case "INSERT":
					createAudit(record, context);
					break;
				case "MODIFY":
					updateAudit(record, context);
					break;
				default:
					context.getLogger().log("ERROR - Event not implemented: " + record.getEventName());
			}
		}
		return null;
	}

	private Table getTargetTable() {
		return dynamoDB.getTable(System.getenv("target_table"));
	}

	private Item buildItemCreate(Map<String, AttributeValue> newImage) {
		Map<String, Object> config = new HashMap<>();
		config.put("key", newImage.get("key").getS());
		config.put("value", Integer.valueOf(newImage.get("value").getN()));
		return new Item()
				.withPrimaryKey("id", UUID.randomUUID().toString())
				.withString("itemKey", newImage.get("key").getS())
				.withString("modificationTime", new DateTime().toString())
				.withMap("newValue", config);
	}

	private void createAudit(DynamodbEvent.DynamodbStreamRecord record, Context context) {
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
		Item item = buildItemCreate(newImage);
		context.getLogger().log("New item: " + item);
		getTargetTable().putItem(item);
	}

	private Item buildItemUpdate(Map<String, AttributeValue> oldImage, Map<String, AttributeValue> newImage) {
		return new Item()
				.withPrimaryKey("id", UUID.randomUUID().toString())
				.withString("itemKey", newImage.get("key").getS())
				.withString("modificationTime", new DateTime().toString())
				.withString("updatedAttribute", "value")
				.withInt("oldValue", Integer.valueOf(oldImage.get("value").getN()))
				.withInt("newValue", Integer.valueOf(newImage.get("value").getN()));
	}

	private void updateAudit(DynamodbEvent.DynamodbStreamRecord record, Context context) {
		Map<String, AttributeValue> oldImage = record.getDynamodb().getOldImage();
		Map<String, AttributeValue> newImage = record.getDynamodb().getNewImage();
		Item item = buildItemUpdate(oldImage, newImage);
		context.getLogger().log("Modified item: " + item);
		getTargetTable().putItem(item);
	}
}
