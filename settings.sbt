import SimpleSettings._

primarySettings := primary(
    name             = "simple-deployment-scheduler"
  , companyName      = "David Hoyt"
  , organization     = "org.github.davidhoyt"
  , homepage         = "https://github.com/davidhoyt/simple-deployment-scheduler"
  , vcsSpecification = "git@github.com:davidhoyt/simple-deployment-scheduler.git"
)

compilerSettings := compiling(
    scalaVersion  = "2.10.3"
  , scalacOptions = Seq("-deprecation", "-unchecked")
)

mavenSettings := maven(
  license(
      name  = "The Apache Software License, Version 2.0"
    , url   = "http://www.apache.org/licenses/LICENSE-2.0.txt"
  ),
  developer(
      id              = "David Hoyt"
    , name            = "David Hoyt"
    , email           = "dhoyt@hoytsoft.org"
    , url             = "http://www.hoytsoft.org/"
    , organization    = "HoytSoft"
    , organizationUri = "http://www.hoytsoft.org/"
    , roles           = Seq("architect", "developer")
  )
)

publishSettings := publishing(
    releaseCredentialsID  = "sonatype-nexus-staging"
  , releaseRepository     = "https://oss.sonatype.org/service/local/staging/deploy/maven2"
  , snapshotCredentialsID = "sonatype-nexus-snapshots"
  , snapshotRepository    = "https://oss.sonatype.org/content/repositories/snapshots"
)
