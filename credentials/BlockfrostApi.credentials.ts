import {
	ICredentialType,
	INodeProperties,
} from 'n8n-workflow';

export class BlockfrostApi implements ICredentialType {
	name = 'blockfrostApi';
	displayName = 'Blockfrost API';
	documentationUrl = 'https://blockfrost.io/';
	properties: INodeProperties[] = [
		{
			displayName: 'Project ID (API Key)',
			name: 'apiKey',
			type: 'string',
			typeOptions: {
				password: true,
			},
			default: '',
			required: true,
			description: 'Your Blockfrost project ID/API key',
		},
	];
}