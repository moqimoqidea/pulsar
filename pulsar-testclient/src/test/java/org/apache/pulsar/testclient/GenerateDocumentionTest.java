/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.pulsar.testclient;

import org.testng.Assert;
import org.testng.annotations.Test;
import picocli.CommandLine;

public class GenerateDocumentionTest {

    @Test
    public void testGenerateDocumention() throws Exception {
        new CmdGenerateDocumentation().run(new String[]{});
    }

    @Test
    public void testSpecifyModuleName() throws Exception {
        String[] args = new String[]{"-n", "produce", "-n", "consume"};
        new CmdGenerateDocumentation().run(args);
    }

    private static final String DESC = "desc";
    @Test
    public void testGetCommandOptionDescription(){
        Arguments arguments = new Arguments();
        CommandLine commander = new CommandLine(arguments);
        String desc = CmdGenerateDocumentation.getCommandDescription(commander);
        Assert.assertEquals(desc, DESC);

        commander.getCommandSpec().options().forEach(option -> {
            String desc1 = CmdGenerateDocumentation.getOptionDescription(option);
            Assert.assertEquals(desc1, DESC);
        });

        ArgumentsWithoutDesc argumentsWithoutDesc = new ArgumentsWithoutDesc();
        commander = new CommandLine(argumentsWithoutDesc);
        desc = CmdGenerateDocumentation.getCommandDescription(commander);
        Assert.assertEquals(desc, "");

        commander.getCommandSpec().options().forEach(option -> {
            String desc1 = CmdGenerateDocumentation.getOptionDescription(option);
            Assert.assertEquals(desc1, "");
        });
    }

    @CommandLine.Command(description = DESC)
    static class Arguments {
        @CommandLine.Option(names = {"-h", "--help"}, description = DESC, help = true)
        boolean help;
    }
    @CommandLine.Command()
    static class ArgumentsWithoutDesc {
        @CommandLine.Option(names = {"-h", "--help"}, help = true)
        boolean help;
    }
}