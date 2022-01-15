using System;
using System.IO;
using System.Threading.Tasks;

using Amazon.Lambda.Core;
using Amazon.Lambda.S3Events;
using Amazon.S3;
using Amazon.S3.Model;
using ImageMagick;

// Assembly attribute to enable the Lambda function's JSON input to be converted into a .NET class.
[assembly: LambdaSerializer(typeof(Amazon.Lambda.Serialization.SystemTextJson.DefaultLambdaJsonSerializer))]

namespace ThumbnailCreator
{
    public class Function
    {
        private const string OutputBucketName = "---YOURBUCKETNAME---";

        IAmazonS3 S3Client { get; set; }

        /// <summary>
        /// Default constructor. This constructor is used by Lambda to construct the instance. When invoked in a Lambda environment
        /// the AWS credentials will come from the IAM role associated with the function and the AWS region will be set to the
        /// region the Lambda function is executed in.
        /// </summary>
        public Function()
        {
            S3Client = new AmazonS3Client();
        }

        /// <summary>
        /// Constructs an instance with a preconfigured S3 client. This can be used for testing the outside of the Lambda environment.
        /// </summary>
        /// <param name="s3Client"></param>
        public Function(IAmazonS3 s3Client)
        {
            S3Client = s3Client;
        }

        /// <summary>
        /// This method is called for every Lambda invocation. This method takes in an S3 event object and can be used 
        /// to respond to S3 notifications.
        /// </summary>
        /// <param name="evnt"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public async Task<string> FunctionHandler(S3Event evnt, ILambdaContext context)
        {
            var s3Event = evnt.Records?[0].S3;
            if (s3Event == null || s3Event.Bucket.Name == OutputBucketName) // avoid recursion
            {
                return null;
            }

            try
            {
                context.Logger.LogLine($"Trying to Get object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");

                // let's get the s3 object from the bucket, we have the bucket name and the object key in the event parameter
                using (GetObjectResponse response = await S3Client.GetObjectAsync(s3Event.Bucket.Name, s3Event.Object.Key))
                {
                    using (Stream responseStream = response.ResponseStream)
                    {
                        using (StreamReader reader = new StreamReader(responseStream))
                        {
                            string contentType = response.Headers["Content-Type"];
                            Console.WriteLine("Content type: {0}", contentType);

                            context.Logger.LogLine($"Get object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");
                            if (s3Event.Object.Key.EndsWith(".jpg")) // you can do any kind of filtering, or remove the condition, and process everything
                            {
                                // Read from the s3 stream
                                using (var collection = new MagickImageCollection(responseStream))
                                {
                                    foreach (var image in collection)
                                    {
                                        image.Resize(200, 0);// let resize with keep aspect ratio option, the height will be automatically calculated
                                        using (MemoryStream memStream = new MemoryStream())
                                        {
                                            // put the image into a memory stream temporary, till we write it to the s3 bucket
                                            image.Write(memStream);
                                            // lets write the resized image to the target bucket with the same name
                                            var outFileName = s3Event.Object.Key; // you can change/manipulate the output filename here if you want to pre/postfix it
                                            await S3Client.PutObjectAsync(new PutObjectRequest() { Key = outFileName, BucketName = OutputBucketName, InputStream = memStream });
                                        }
                                    }
                                }
                                return response.Headers.ContentType;
                            }
                        }
                    }
                }
            }
            catch (Exception e)
            {
                context.Logger.LogLine($"Error processing object {s3Event.Object.Key} from bucket {s3Event.Bucket.Name}.");
                context.Logger.LogLine(e.Message);
                context.Logger.LogLine(e.StackTrace);
                throw;
            }
            return "";
        }
    }
}
