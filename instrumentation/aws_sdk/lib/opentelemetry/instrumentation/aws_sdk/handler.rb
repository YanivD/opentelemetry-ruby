# frozen_string_literal: true

# Copyright The OpenTelemetry Authors
#
# SPDX-License-Identifier: Apache-2.0

module OpenTelemetry
  module Instrumentation
    module AwsSdk
      # Generates Spans for all interactions with AwsSdk
      class Handler < Seahorse::Client::Handler
        SQS_SEND_MESSAGE = 'SQS.SendMessage'
        SQS_SEND_MESSAGE_BATCH = 'SQS.SendMessageBatch'
        SQS_RECEIVE_MESSAGE = 'SQS.ReceiveMessage'
        SNS_PUBLISH = 'SNS.Publish'

        def call(context)
          span_name = get_span_name(context)
          attributes = get_span_attributes(context)

          tracer.in_span(span_name, kind: get_span_kind(context), attributes: attributes) do |span|
            execute = proc {
              inject_context(context)
              super(context).tap do |response|
                if (err = response.error)
                  span.record_exception(err)
                  span.status = Trace::Status.error(err)
                end
              end
            }

            if instrumentation_config[:suppress_internal_instrumentation]
              OpenTelemetry::Common::Utilities.untraced(&execute)
            else
              execute.call
            end
          end
        end

        def inject_context(context)
          client_method = get_client_method(context)
          return unless [SQS_SEND_MESSAGE, SQS_SEND_MESSAGE_BATCH, SNS_PUBLISH].include? client_method

          context.params[:message_attributes] ||= {}
          OpenTelemetry.propagation.inject(context.params[:message_attributes], setter: MessageAttributeSetter)
        end

        def get_span_attributes(context)
          span_attributes = {
            'aws.region' => context.config.region,
            OpenTelemetry::SemanticConventions::Trace::RPC_SYSTEM => 'aws-api',
            OpenTelemetry::SemanticConventions::Trace::RPC_METHOD => get_operation(context),
            OpenTelemetry::SemanticConventions::Trace::RPC_SERVICE => get_service_name(context)
          }

          messaging_attributes = MessagingHelper.get_messaging_attributes(context, get_service_name(context), get_operation(context))
          db_attributes = DbHelper.get_db_attributes(context, get_service_name(context), get_operation(context))
          span_attributes.merge(messaging_attributes).merge(db_attributes)
        end

        def get_service_name(context)
          context&.client.class.api.metadata['serviceId'] || context&.client.class.to_s.split('::')[1]
        end

        def get_operation(context)
          context&.operation&.name
        end

        def get_span_kind(context)
          client_method = get_client_method(context)
          case client_method
          when SQS_SEND_MESSAGE, SQS_SEND_MESSAGE_BATCH, SNS_PUBLISH
            OpenTelemetry::Trace::SpanKind::PRODUCER
          when SQS_RECEIVE_MESSAGE
            OpenTelemetry::Trace::SpanKind::CONSUMER
          else
            OpenTelemetry::Trace::SpanKind::CLIENT
          end
        end

        def get_span_name(context)
          client_method = get_client_method(context)
          case client_method
          when SQS_SEND_MESSAGE, SQS_SEND_MESSAGE_BATCH, SNS_PUBLISH
            "#{MessagingHelper.get_queue_name(context)} send"
          when SQS_RECEIVE_MESSAGE
            "#{MessagingHelper.get_queue_name(context)} receive"
          else
            client_method
          end
        end

        def get_client_method(context)
          "#{get_service_name(context)}.#{get_operation(context)}"
        end

        def tracer
          AwsSdk::Instrumentation.instance.tracer
        end

        def instrumentation_config
          AwsSdk::Instrumentation.instance.config
        end
      end

      # A Seahorse::Client::Plugin that enables instrumentation for all AWS services
      class Plugin < Seahorse::Client::Plugin
        def add_handlers(handlers, config)
          # run before Seahorse::Client::Plugin::ParamValidator (priority 50)
          handlers.add Handler, step: :validate, priority: 49
        end
      end
    end
  end
end
