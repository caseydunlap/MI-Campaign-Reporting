# Michigan Campaign Reporting

## Overview

**IL Monthly Report** is a reporting automation tool designed to calculate and report various customer support metrics on a monthly basis. The tool streamlines report creation, adheres to a predictable schedule, and provides increased transparency to state stakeholders.

## Setup and Installation

Since there is no specific setup or installation process required, this output from this script will be delivered to internal HHA stakeholders for further delivery to external state stakeholders.

## Usage

The script runs once a month, specifically on the 1st of each month at 8am EST. Reports are generated and sent via email.

## File Tabs
### Statuses

Contains definitions for each applicable stage for ticket project types (HHA, RCOSD, ESD, EISD, EAS).

### JIRA

A comprehensive report of all tickets created in Illinois within the reporting month, including the following fields:
  - key: A unique identifier for each ticket.
  - reporter: External party who raised the issue with HHAeXchange.
  - hhax_platform_region_tag: Internal geographic identifier tag associated with the provider who raised the issue with HHAeXchange.
  - state: State associated with the provider who raised the issue with HHAeXchange.
  - primary_location: State associated with the provider who raised the issue with HHAeXchange.
  - hhax_market: State associated with the provider who raised the issue with HHAeXchange.
  -  associations: Provider associated with the issue raised with HHAeXchange.
  - create_date: The date the issue was created in JIRA.
  - resolved_date: The date the issue was resolved in JIRA.
  - updated: The date of the last update made on the issue at the time of report compilation.
  - payer: Payers associated with the provider who raised the issue with HHAeXchange.
  - status: Status of the JIRA issue at the time of report compilation.
  - summary: Brief summary of the issues.
  - tax_id: Federal tax number associated with the provider who raised the issue with HHAeXchange.
  - closed_via_automation: Boolean indicating if a ticket was closed due to provider non-response.
  - response_time_(mins): The amount of time, in minutes, it took for HHAeXchange to respond to the provider's issue.

### JIRA Summary

A report summarizing tickets created by day within the reporting period, pivoted by project type. It includes a 'closed' column, capturing the number of tickets created on that day that are closed or resolved as of the time of report compilation.

### AWS

A report aggregating the number of contacts queued, contacts abandoned, abandoned rate, and average answer time in minutes in the Illinois dedicated queue during the reporting month. The abandoned rate is calculated as contacts abandoned divided by contacts queued.

## Known Issues

No known issues at the moment.

## Contacts

For any questions or inquiries, please contact your internal HHA stakeholders.
