# frozen_string_literal: true

# Copyright The OpenTelemetry Authors
#
# SPDX-License-Identifier: Apache-2.0

module OpenTelemetry
  module Instrumentation
    module AwsSdk
      # MessagingHelper class provides methods for calculating messaging span attributes
      class MessagingHelper
        class << self
          def get_queue_name(context)
            topic_arn = context.metadata[:original_params][:topic_arn]
            target_arn = context.metadata[:original_params][:target_arn]
            phone_number = context.metadata[:original_params][:phone_number]
            queue_url = context.metadata[:original_params][:queue_url]

            if topic_arn || target_arn
              arn = topic_arn || target_arn
              begin
                arn.split(':')[-1]
              rescue StandardError
                arn
              end
            end

            return phone_number if phone_number

            return queue_url.split('/')[-1] if queue_url

            'unknown'
          end

          def get_messaging_attributes(context, service_name, operation)
            return get_sns_attributes(context, operation) if service_name == 'SNS'
            return get_sqs_attributes(context, operation) if service_name == 'SQS'
            {}
          end

          def get_sqs_attributes(context, operation)
            sqs_attributes = {
              SemanticConventions::Trace::MESSAGING_SYSTEM => 'aws.sqs',
              SemanticConventions::Trace::MESSAGING_DESTINATION_KIND => 'queue',
              SemanticConventions::Trace::MESSAGING_DESTINATION => get_queue_name(context),
              SemanticConventions::Trace::MESSAGING_URL => context.metadata[:original_params][:queue_url]
            }

            sqs_attributes[SemanticConventions::Trace::MESSAGING_OPERATION] = 'receive' if operation == 'ReceiveMessage'

            sqs_attributes
          end

          def get_sns_attributes(context, operation)
            sns_attributes = {
              SemanticConventions::Trace::MESSAGING_SYSTEM => 'aws.sns'
            }

            if operation == 'Publish'
              sns_attributes[SemanticConventions::Trace::MESSAGING_DESTINATION_KIND] = 'topic'
              sns_attributes[SemanticConventions::Trace::MESSAGING_DESTINATION] = get_queue_name(context)
            end

            sns_attributes
          end
        end
      end
    end
  end
end
