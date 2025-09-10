import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	NodeConnectionType,
} from 'n8n-workflow';

export class AdaHandleResolver implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'Crowd Handle',
		name: 'adaHandleResolver',
		icon: 'file:crowd-handle.png',
		group: ['transform'],
		version: 1,
		description: 'Extract handle details through the ADA handle public API',
		defaults: {
			name: 'Crowd Handle',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		properties: [
			{
				displayName: 'Handle Name',
				name: 'handleName',
				type: 'string',
				default: '',
				required: true,
				placeholder: 'crowdtrendz (without $ prefix)',
				description: 'The ADA handle name to resolve (without $ prefix)',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		for (let i = 0; i < items.length; i++) {
			try {
				const handleName = this.getNodeParameter('handleName', i) as string;

				// Clean the handle name (remove $ if present)
				let cleanHandleName = handleName.trim();
				if (cleanHandleName.startsWith('$')) {
					cleanHandleName = cleanHandleName.substring(1);
				}

				if (!cleanHandleName) {
					throw new Error('Handle name cannot be empty');
				}

				// Make HTTP request to Handle.me API
				const url = `https://api.handle.me/handles/${cleanHandleName}`;

				const response = await this.helpers.request({
					method: 'GET',
					url: url,
					json: true,
				});

				// Extract the resolved address from the response
				let cardanoAddress = null;
				let status = 'not_found';

				if (response && response.resolved_addresses && response.resolved_addresses.ada) {
					cardanoAddress = response.resolved_addresses.ada;
					status = 'success';
				}

				returnData.push({
					json: {
						handleName: `$${cleanHandleName}`,
						cleanHandleName,
						cardanoAddress,
						status,
						apiResponse: response,
						resolvedWith: 'handle.me_api',
					},
				});

			} catch (error: any) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';

				if (this.continueOnFail()) {
					returnData.push({
						json: {
							handleName: this.getNodeParameter('handleName', i),
							cleanHandleName: null,
							cardanoAddress: null,
							status: 'error',
							error: errorMessage,
						},
					});
				} else {
					throw new NodeOperationError(this.getNode(), `ADA Handle resolution failed: ${errorMessage}`);
				}
			}
		}

		return [returnData];
	}
}