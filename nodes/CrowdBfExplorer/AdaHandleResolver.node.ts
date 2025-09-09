import {
	IExecuteFunctions,
	INodeExecutionData,
	INodeType,
	INodeTypeDescription,
	NodeOperationError,
	NodeConnectionType,
} from 'n8n-workflow';

// Import Mesh SDK - using require to avoid TypeScript import issues
const { BlockfrostProvider } = require('@meshsdk/core');

interface HandleResolutionResult {
	handleName: string;
	cleanHandleName: string;
	cardanoAddress: string | null;
	status: 'success' | 'not_found' | 'error';
	resolvedWith?: string;
	isSubHandle?: boolean;
	multipleHolders?: boolean;
	allHolders?: Array<{ address: string; quantity: string }>;
	metadata?: any;
	note?: string;
	error?: string;
}

export class AdaHandleResolver implements INodeType {
	description: INodeTypeDescription = {
		displayName: 'ADA Handle Resolver',
		name: 'adaHandleResolver',
		icon: 'fa:link',
		group: ['transform'],
		version: 1,
		subtitle: '={{$parameter["operation"]}}',
		description: 'Resolve ADA handles to Cardano addresses using Mesh SDK',
		defaults: {
			name: 'ADA Handle Resolver',
		},
		inputs: [NodeConnectionType.Main],
		outputs: [NodeConnectionType.Main],
		credentials: [
			{
				name: 'blockfrostApi',
				required: true,
			},
		],
		properties: [
			{
				displayName: 'Operation',
				name: 'operation',
				type: 'options',
				noDataExpression: true,
				options: [
					{
						name: 'Resolve Handle',
						value: 'resolveHandle',
						description: 'Resolve ADA handle to blockchain address',
					},
					{
						name: 'Bulk Resolve',
						value: 'bulkResolve',
						description: 'Resolve multiple handles from input data',
					},
					{
						name: 'Validate Handle Format',
						value: 'validateHandle',
						description: 'Check if handle name follows ADA handle format rules',
					},
				],
				default: 'resolveHandle',
			},
			{
				displayName: 'Network',
				name: 'network',
				type: 'options',
				options: [
					{
						name: 'Mainnet',
						value: 'mainnet',
					},
					{
						name: 'Testnet',
						value: 'testnet',
					},
				],
				default: 'mainnet',
				description: 'Cardano network to use',
			},
			{
				displayName: 'Handle Name',
				name: 'handleName',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['resolveHandle'],
					},
				},
				default: '',
				required: true,
				placeholder: '$spitzer or spitzer',
				description: 'The ADA handle name to resolve (with or without $ prefix)',
			},
			{
				displayName: 'Handle Field Name',
				name: 'handleField',
				type: 'string',
				displayOptions: {
					show: {
						operation: ['bulkResolve'],
					},
				},
				default: 'handle',
				required: true,
				description: 'Name of the field containing handle names in input data',
			},
			{
				displayName: 'Include Metadata',
				name: 'includeMetadata',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['resolveHandle', 'bulkResolve'],
					},
				},
				default: false,
				description: 'Whether to include additional metadata in the response',
			},
			{
				displayName: 'Continue on Error',
				name: 'continueOnError',
				type: 'boolean',
				displayOptions: {
					show: {
						operation: ['bulkResolve'],
					},
				},
				default: true,
				description: 'Continue processing other handles if one fails',
			},
		],
	};

	async execute(this: IExecuteFunctions): Promise<INodeExecutionData[][]> {
		const items = this.getInputData();
		const returnData: INodeExecutionData[] = [];

		// Get credentials
		const credentials = await this.getCredentials('blockfrostApi');
		const blockfrostApiKey = credentials.apiKey as string;

		for (let i = 0; i < items.length; i++) {
			try {
				const operation = this.getNodeParameter('operation', i) as string;
				const network = this.getNodeParameter('network', i) as string;

				// Initialize Mesh provider
				const provider = new BlockfrostProvider(blockfrostApiKey);

				switch (operation) {
					case 'resolveHandle':
						const handleName = this.getNodeParameter('handleName', i) as string;
						const includeMetadata = this.getNodeParameter('includeMetadata', i) as boolean;

						const result = await AdaHandleResolver.resolveHandle(
							provider,
							handleName,
							network,
							includeMetadata
						);

						returnData.push({
							json: {
								operation: 'resolveHandle',
								network,
								...result,
							},
						});
						break;

					case 'bulkResolve':
						const handleField = this.getNodeParameter('handleField', i) as string;
						const includeBulkMetadata = this.getNodeParameter('includeMetadata', i) as boolean;
						const continueOnError = this.getNodeParameter('continueOnError', i) as boolean;

						const inputHandle = items[i].json[handleField] as string;
						if (!inputHandle) {
							if (continueOnError) {
								returnData.push({
									json: {
										operation: 'bulkResolve',
										network,
										originalData: items[i].json,
										handleName: null,
										cardanoAddress: null,
										status: 'error',
										error: `No handle found in field '${handleField}'`,
									},
								});
								continue;
							} else {
								throw new Error(`No handle found in field '${handleField}' for item ${i}`);
							}
						}

						try {
							const bulkResult = await AdaHandleResolver.resolveHandle(
								provider,
								inputHandle,
								network,
								includeBulkMetadata
							);

							returnData.push({
								json: {
									operation: 'bulkResolve',
									network,
									originalData: items[i].json,
									...bulkResult,
								},
							});
						} catch (error: any) {
							if (continueOnError) {
								returnData.push({
									json: {
										operation: 'bulkResolve',
										network,
										originalData: items[i].json,
										handleName: inputHandle,
										cardanoAddress: null,
										status: 'error',
										error: error.message,
									},
								});
							} else {
								throw error;
							}
						}
						break;

					case 'validateHandle':
						const validateHandle = this.getNodeParameter('handleName', i) as string;
						const validation = AdaHandleResolver.validateHandleFormat(validateHandle);

						returnData.push({
							json: {
								operation: 'validateHandle',
								handleName: validateHandle,
								...validation,
							},
						});
						break;
				}

			} catch (error: any) {
				const errorMessage = error instanceof Error ? error.message : 'Unknown error occurred';

				if (this.continueOnFail()) {
					returnData.push({
						json: {
							success: false,
							error: errorMessage,
							operation: this.getNodeParameter('operation', i),
							network: this.getNodeParameter('network', i),
						},
					});
				} else {
					throw new NodeOperationError(this.getNode(), `ADA Handle resolution failed: ${errorMessage}`);
				}
			}
		}

		return [returnData];
	}

	/**
	 * Resolve ADA handle to blockchain address using Mesh SDK
	 */
	private static async resolveHandle(
		provider: any,
		handleName: string,
		network: string,
		includeMetadata: boolean = false
	): Promise<HandleResolutionResult> {
		// Clean the handle name
		let cleanHandleName = handleName.trim();
		if (cleanHandleName.startsWith('$')) {
			cleanHandleName = cleanHandleName.substring(1);
		}

		// Validate handle format
		const validation = AdaHandleResolver.validateHandleFormat(cleanHandleName);
		if (!validation.isValid) {
			return {
				handleName,
				cleanHandleName,
				cardanoAddress: null,
				status: 'error',
				error: validation.errors.join(', '),
			};
		}

		try {
			// Use Mesh SDK's dedicated fetchHandleAddress method
			const cardanoAddress = await provider.fetchHandleAddress(cleanHandleName);

			if (cardanoAddress) {
				const result: HandleResolutionResult = {
					handleName,
					cleanHandleName,
					cardanoAddress,
					status: 'success',
					resolvedWith: 'mesh_fetchHandleAddress',
					isSubHandle: cleanHandleName.includes('@'),
				};

				// Include metadata if requested
				if (includeMetadata) {
					try {
						// Fetch additional handle metadata
						const handleMetadata = await provider.fetchHandle(cleanHandleName);

						result.metadata = {
							handleMetadata,
							resolvedAt: new Date().toISOString(),
							network,
							method: 'fetchHandleAddress',
						};
					} catch (metadataError) {
						// Metadata fetch failed, but we still have the address
						result.metadata = {
							resolvedAt: new Date().toISOString(),
							network,
							method: 'fetchHandleAddress',
							metadataError: 'Could not fetch handle metadata',
						};
					}
				}

				return result;
			}
		} catch (error: any) {
			// Handle not found or other error
			return {
				handleName,
				cleanHandleName,
				cardanoAddress: null,
				status: 'not_found',
				error: `Handle not found: ${error.message || 'Unknown error'}`,
			};
		}

		// Fallback: Handle not found
		return {
			handleName,
			cleanHandleName,
			cardanoAddress: null,
			status: 'not_found',
			error: 'Handle not found',
		};
	}

	/**
	 * Validate ADA handle format
	 */
	private static validateHandleFormat(handleName: string): { isValid: boolean; errors: string[] } {
		const errors: string[] = [];

		// Remove $ prefix for validation
		let cleanName = handleName.trim();
		if (cleanName.startsWith('$')) {
			cleanName = cleanName.substring(1);
		}

		// Check if empty
		if (!cleanName) {
			errors.push('Handle name cannot be empty');
			return { isValid: false, errors };
		}

		// Check length (ADA handles are typically 1-15 characters)
		if (cleanName.length < 1) {
			errors.push('Handle name must be at least 1 character');
		}
		if (cleanName.length > 15) {
			errors.push('Handle name cannot exceed 15 characters');
		}

		// Check for valid characters (alphanumeric, hyphen, underscore)
		// SubHandles can contain @ symbol
		const isSubHandle = cleanName.includes('@');
		const validPattern = isSubHandle
			? /^[a-zA-Z0-9_-]+@[a-zA-Z0-9_-]+$/
			: /^[a-zA-Z0-9_-]+$/;

		if (!validPattern.test(cleanName)) {
			if (isSubHandle) {
				errors.push('SubHandle format should be: name@domain (alphanumeric, hyphen, underscore only)');
			} else {
				errors.push('Handle name can only contain letters, numbers, hyphens, and underscores');
			}
		}

		// SubHandle specific validation
		if (isSubHandle) {
			const parts = cleanName.split('@');
			if (parts.length !== 2) {
				errors.push('SubHandle must contain exactly one @ symbol');
			} else {
				const [name, domain] = parts;
				if (!name) {
					errors.push('SubHandle name part cannot be empty');
				}
				if (!domain) {
					errors.push('SubHandle domain part cannot be empty');
				}
				if (name.length > 8) {
					errors.push('SubHandle name part cannot exceed 8 characters');
				}
				if (domain.length > 8) {
					errors.push('SubHandle domain part cannot exceed 8 characters');
				}
			}
		}

		// Check for reserved words or patterns
		const reservedPatterns = ['admin', 'root', 'system', 'cardano', 'ada'];
		if (reservedPatterns.some(pattern => cleanName.toLowerCase().includes(pattern))) {
			errors.push('Handle name contains reserved words');
		}

		return {
			isValid: errors.length === 0,
			errors
		};
	}
}