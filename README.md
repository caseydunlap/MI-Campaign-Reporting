# Michigan Campaign Reporting

## Overview

**Michigan Campaign Reporting** is a reporting automation tool designed to calculate and report various metrics associated with the campaign onboarding journey. The tool streamlines report creation, adheres to a predictable schedule, and provides increased transparency to state stakeholders.

## Setup and Installation

Since there is no specific setup or installation process required, this output from this script will be delivered to internal HHA stakeholders for further delivery to external state stakeholders.

## Usage

The script runs once a day at 930 am EST. Outputs are posted to internal Sharepoints folders, and Snowflake tables.

## Methodology
### Technology Stack
The tech stack utilized in the report compilation includes:
  - Python
  - AWS Glue

### Data Sources
The data sources utilized in the report compilation:
  - Internal Microsoft Excel Files
  - HHAeXchange
  - Cognito Forms
  - Docebo
  - Zoom
  - HubSpot
    
### Identifiers
  - For fields determined by a logical test, the federal tax number is used as the identifier.

## Field Definitions
### Fields
- PROVIDER_TAX_ID: Federal tax number associated with provider, as provided to HHaExchange by state counterparties.
- PROVIDER_NAME: Provider name, as provided to HHAeXchange by state counterparties.
- PROVIDER_NPI_NUMBER: Assigned Provider NPI number, as provided to HHAeXchange by state counterparties.
- TAX_ID_NPI: Concatenation of PROVIDER_TAX_ID and PROVIDER_NPI_NUMBER.
- PROVIDER_ADDRESS_1: Provider address line 1, as provided to HHAeXchange by state counterparties.
- PROVIDER_CITY: Provider address city, as provided to HHAeXchange by state counterparties.
- PROVIDER_STATE: Provider address state, as provided to HHAeXchange by state counterparties.
- PROVIDER_ZIP_CODE: Provider address zip code (length varies), as provided to HHAeXchange by state counterparties.
- PROVIDER_CONTACT_NAME: Provider contact name, as provided to HHAeXchange by state counterparties.
- PROVIDER_EMAIL_ADDRESS: Provider email address, as provided to HHAeXchange by state counterparties.
- PROVIDER_PHONE_NUMBER: Provider phone number, as provided to HHAeXchange by state counterparties.
- IN_HHAX: Field to determine if provider is currently in HHAeXchange.
- WAVE: Campaign wave identifier.
- ATTENDED_INFO_SESSION: Boolean indicating if a provider attended home health campaign wave informational session training webinar.
- REGISTERED_INFO_SESSION: Boolean, indicating if a provider registered for home health campaign wave informational session training webinar.
- ATTENDED_EDI_SESSION: Boolean indicating if a provider attended home health campaign wave EDI provider onboarding webinar.
- REGISTERED_EDI_SESSION: Boolean indicating if a provider registered for home health campaign wave EDI provider onboarding webinar.
- ATTENDED_SUT_SESSION: Boolean indicating if a provider attended home health campaign wave system user training webinar.
- REGISTERED_SUT_SESSION: Boolean indicating if a provider registered for home health campaign wave system user training webinar.
- ATTENDED_GS_SESSION: Boolean indicating if a provider attended home health campaign wave getting started webinar.
- REGISTERED_GS_SESSION: Boolean indicating if a provider registered for home health campaign wave getting started webinar.
- ATTENDED_OH_SESSION: Boolean indicating if a provider attended home health campaign wave open hours webinar.
- REGISTERED_OH_SESSION: Boolean indicating if a provider registered for home health campaign wave open hours webinar.
- ATTENDED_HH_INFO_SESSION: Boolean indicating if a provider attended home help campaign wave informational session training webinar.
- REGISTERED_HH_INFO_SESSION: Boolean, indicating if a provider registered for home help campaign wave informational session training webinar.
- ATTENDED_HH_EDI_SESSION: Boolean indicating if a provider attended home help campaign wave EDI provider onboarding webinar.
- REGISTERED_HH_EDI_SESSION: Boolean indicating if a provider registered for home help campaign wave EDI provider onboarding webinar.
- ATTENDED_HH_SUT_SESSION: Boolean indicating if a provider attended home help campaign wave system user training webinar.
- REGISTERED_HH_SUT_SESSION: Boolean indicating if a provider registered for home help campaign wave system user training webinar.
- ATTENDED_HH_GS_SESSION: Boolean indicating if a provider attended home help campaign wave getting started webinar.
- REGISTERED_HH_GS_SESSION: Boolean indicating if a provider registered for home help campaign wave getting started webinar.
- ATTENDED_HH_OH_SESSION: Boolean indicating if a provider attended home help campaign wave open hours webinar.
- REGISTERED_HH_OH_SESSION: Boolean indicating if a provider registered for home help campaign wave open hours webinar.
- ATTENDED_PCS_GR_SESSION: Boolean indicating if a provider attended pcs campaign wave getting ready for evv webinar.
- REGISTERED_PCS_GR_SESSION: Boolean indicating if a provider registered for pcs campaign wave getting ready for evv webinar.
- LEARNING_PLAN_ENROLLMENT_STATUS: Provider Learning Plan enrollment status for home health campaign wave learning plan.
- LEARNING_PLAN_ENROLLMENT_STATUS_HH: Provider Learning Plan enrollment status for home help campaign wave learning plan.
- ATTENDED_PCS_INFO_SESSION_INPERSON: Boolean indicating if a provider attended pcs campaign wave info s in person event.
- REGISTERED_PCS_INFO_SESSION_INPERSON: Boolean indicating if a provider regsistered for pcs campaign wave in person event.
- ATTENDED_PCS_INFO_SESSION_WEBINAR: Boolean indicating if a provider attended pcs campaign wave informational session webinar.
- REGISTERED_PCS_INFO_SESSION_WEBINAR: Boolean indicating if a provider regsistered for pcs campaign wave informational session webinar.
- EVV_SYSTEM_CHOICE: Provider EVV election as reflected on their cognito form.
- PORTAL_CREATED: Boolean indicating if a provider has had their portal created.
- PORTAL_TYPE: Field indicating if customer is paid or free user.
- EVENT_DATE: Date of report compilation.
- PROVIDER_ID: Environment specific provider id.
- PAYERS: Payers associated with the provider.

## Known Issues

No known issues at the moment.

## Contacts

For any questions or inquiries, please contact your internal HHAeXchange stakeholders.
