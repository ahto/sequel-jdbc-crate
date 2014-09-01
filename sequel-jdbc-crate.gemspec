# -*- encoding: utf-8 -*-

Gem::Specification.new do |gem|
  gem.name = %q{sequel-jdbc-crate}
  gem.version = "0.0.2"

  gem.authors = ['Ahto Jussila']
  gem.email = ['ahto@jussila.org']
  gem.homepage = 'http://github.com/ahto/sequel-jdbc-crate'
  gem.licenses = ['Apache-2']

  gem.files = [ 'README.md', 'LICENSE.txt', 'examples.rb', *Dir['lib/**/*'].to_a ]

  gem.rdoc_options = ["--main", "README.md"]
  gem.require_paths = ["lib"]

  gem.summary = %q{Crate jdbc for Sequel.}
  gem.description = %q{Sequel JDBC adapter for Crate}

  gem.add_dependency 'sequel', '~> 4.13.0'
  gem.add_dependency 'jdbc-crate', '~> 1.0.5.4'

  gem.add_development_dependency 'activesupport', "~> 4.0.4"
end
