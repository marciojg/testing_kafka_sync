# frozen_string_literal: true

ENV["BUNDLE_GEMFILE"] ||= File.expand_path("../Gemfile", __dir__)

require "bundler/setup"
require "dotenv/load"
Bundler.require(:default)

Dir.glob(File.join(__dir__, "..", "app", "**", "*.rb")).sort.each { |file| require file }
