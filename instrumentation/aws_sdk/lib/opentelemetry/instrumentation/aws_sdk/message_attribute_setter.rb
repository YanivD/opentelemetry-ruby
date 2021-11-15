# frozen_string_literal: true

# Copyright The OpenTelemetry Authors
#
# SPDX-License-Identifier: Apache-2.0

module OpenTelemetry
  module Instrumentation
    module AwsSdk
      # The MessageAttributeSetter class provides methods for writing tracing information to
      # SNS / SQS messages.
      #
      # @example
      #   OpenTelemetry.propagation.inject(context.params[:message_attributes], setter: MessageAttributeSetter)
      class MessageAttributeSetter
        def self.set(carrier, key, value)
          # TODO: move to aws_sdk handler after resolved: https://github.com/open-telemetry/opentelemetry-ruby/issues/1019
          # https://docs.aws.amazon.com/AWSSimpleQueueService/latest/SQSDeveloperGuide/sqs-quotas.html
          if carrier.length < 10
            carrier[key] = { string_value: value, data_type: 'String' }
          else
            OpenTelemetry.logger.warn('aws-sdk instrumentation: cannot set context propagation on SQS/SNS message due to maximum amount of MessageAttributes')
          end
        end
      end
    end
  end
end
